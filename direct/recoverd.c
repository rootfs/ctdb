/* 
   ctdb recovery daemon

   Copyright (C) Ronnie Sahlberg  2007

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include "includes.h"
#include "lib/events/events.h"
#include "system/filesys.h"
#include "popt.h"
#include "cmdline.h"
#include "../include/ctdb.h"
#include "../include/ctdb_private.h"

static int timed_out = 0;

/*
  show usage message
 */
static void usage(void)
{
	printf(
		"Usage: recoverd\n"
		);
	exit(1);
}

static void timeout_func(struct event_context *ev, struct timed_event *te, 
	struct timeval t, void *private_data)
{
	timed_out = 1;
}

static int set_recovery_mode(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t rec_mode)
{
	int j, ret;

	/* set recovery mode to active on all nodes */
	for (j=0; j<nodemap->num; j++) {
		/* dont change it for nodes that are unavailable */
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}

		ret = ctdb_ctrl_setrecmode(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, rec_mode);
		if (ret != 0) {
			printf("Unable to set recmode on node %u\n", nodemap->nodes[j].vnn);
			return -1;
		}
	}

	return 0;
}

static int create_missing_remote_databases(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t vnn, struct ctdb_dbid_map *dbmap, TALLOC_CTX *mem_ctx)
{
	int i, j, db, ret;
	struct ctdb_dbid_map *remote_dbmap;

	/* verify that all other nodes have all our databases */
	for (j=0; j<nodemap->num; j++) {
		/* we dont need to ourself ourselves */
		if (nodemap->nodes[j].vnn == vnn) {
			continue;
		}
		/* dont check nodes that are unavailable */
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}

		ret = ctdb_ctrl_getdbmap(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, &remote_dbmap);
		if (ret != 0) {
			printf("Unable to get dbids from node %u\n", vnn);
			return -1;
		}

		/* step through all local databases */
		for (db=0; db<dbmap->num;db++) {
			const char *name;


			for (i=0;i<remote_dbmap->num;i++) {
				if (dbmap->dbids[db] == remote_dbmap->dbids[i]) {
					break;
				}
			}
			/* the remote node already have this database */
			if (i!=remote_dbmap->num) {
				continue;
			}
			/* ok so we need to create this database */
			ctdb_ctrl_getdbname(ctdb, timeval_current_ofs(1, 0), vnn, dbmap->dbids[db], mem_ctx, &name);
			if (ret != 0) {
				printf("Unable to get dbname from node %u\n", vnn);
				return -1;
			}
			ctdb_ctrl_createdb(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, name);
			if (ret != 0) {
				printf("Unable to create remote db:%s\n", name);
				return -1;
			}
		}
	}

	return 0;
}


static int create_missing_local_databases(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t vnn, struct ctdb_dbid_map **dbmap, TALLOC_CTX *mem_ctx)
{
	int i, j, db, ret;
	struct ctdb_dbid_map *remote_dbmap;

	/* verify that we have all database any other node has */
	for (j=0; j<nodemap->num; j++) {
		/* we dont need to ourself ourselves */
		if (nodemap->nodes[j].vnn == vnn) {
			continue;
		}
		/* dont check nodes that are unavailable */
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}

		ret = ctdb_ctrl_getdbmap(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, &remote_dbmap);
		if (ret != 0) {
			printf("Unable to get dbids from node %u\n", vnn);
			return -1;
		}

		/* step through all databases on the remote node */
		for (db=0; db<remote_dbmap->num;db++) {
			const char *name;

			for (i=0;i<(*dbmap)->num;i++) {
				if (remote_dbmap->dbids[db] == (*dbmap)->dbids[i]) {
					break;
				}
			}
			/* we already have this db locally */
			if (i!=(*dbmap)->num) {
				continue;
			}
			/* ok so we need to create this database and
			   rebuild dbmap
			 */
			ctdb_ctrl_getdbname(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, remote_dbmap->dbids[db], mem_ctx, &name);
			if (ret != 0) {
				printf("Unable to get dbname from node %u\n", nodemap->nodes[j].vnn);
				return -1;
			}
			ctdb_ctrl_createdb(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, name);
			if (ret != 0) {
				printf("Unable to create local db:%s\n", name);
				return -1;
			}
			ret = ctdb_ctrl_getdbmap(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, dbmap);
			if (ret != 0) {
				printf("Unable to reread dbmap on node %u\n", vnn);
				return -1;
			}
		}
	}

	return 0;
}


static int pull_all_remote_databases(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t vnn, struct ctdb_dbid_map *dbmap, TALLOC_CTX *mem_ctx)
{
	int i, j, ret;

	/* pull all records from all other nodes across onto this node
	   (this merges based on rsn)
	*/
	for (i=0;i<dbmap->num;i++) {
		for (j=0; j<nodemap->num; j++) {
			/* we dont need to merge with ourselves */
			if (nodemap->nodes[j].vnn == vnn) {
				continue;
			}
			/* dont merge from nodes that are unavailable */
			if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
				continue;
			}
			ret = ctdb_ctrl_copydb(ctdb, timeval_current_ofs(2, 0), nodemap->nodes[j].vnn, vnn, dbmap->dbids[i], CTDB_LMASTER_ANY, mem_ctx);
			if (ret != 0) {
				printf("Unable to copy db from node %u to node %u\n", nodemap->nodes[j].vnn, vnn);
				return -1;
			}
		}
	}

	return 0;
}



static int update_dmaster_on_all_databases(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t vnn, struct ctdb_dbid_map *dbmap, TALLOC_CTX *mem_ctx)
{
	int i, j, ret;

	/* update dmaster to point to this node for all databases/nodes */
	for (i=0;i<dbmap->num;i++) {
		for (j=0; j<nodemap->num; j++) {
			/* dont repoint nodes that are unavailable */
			if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
				continue;
			}
			ret = ctdb_ctrl_setdmaster(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, ctdb, dbmap->dbids[i], vnn);
			if (ret != 0) {
				printf("Unable to set dmaster for node %u db:0x%08x\n", nodemap->nodes[j].vnn, dbmap->dbids[i]);
				return -1;
			}
		}
	}

	return 0;
}


static int push_all_local_databases(struct ctdb_context *ctdb, struct ctdb_node_map *nodemap, uint32_t vnn, struct ctdb_dbid_map *dbmap, TALLOC_CTX *mem_ctx)
{
	int i, j, ret;

	/* push all records out to the nodes again */
	for (i=0;i<dbmap->num;i++) {
		for (j=0; j<nodemap->num; j++) {
			/* we dont need to push to ourselves */
			if (nodemap->nodes[j].vnn == vnn) {
				continue;
			}
			/* dont push to nodes that are unavailable */
			if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
				continue;
			}
			ret = ctdb_ctrl_copydb(ctdb, timeval_current_ofs(1, 0), vnn, nodemap->nodes[j].vnn, dbmap->dbids[i], CTDB_LMASTER_ANY, mem_ctx);
			if (ret != 0) {
				printf("Unable to copy db from node %u to node %u\n", vnn, nodemap->nodes[j].vnn);
				return -1;
			}
		}
	}

	return 0;
}


static int do_recovery(struct ctdb_context *ctdb, struct event_context *ev,
	TALLOC_CTX *mem_ctx, uint32_t vnn, uint32_t num_active,
	struct ctdb_node_map *nodemap, struct ctdb_vnn_map *vnnmap)
{
	int i, j, ret;
	uint32_t generation;
	struct ctdb_dbid_map *dbmap;

	printf("we need to do recovery !!!\n");

	/* pick a new generation number */
	generation = random();

	/* change the vnnmap on this node to use the new generation 
	   number but not on any other nodes.
	   this guarantees that if we abort the recovery prematurely
	   for some reason (a node stops responding?)
	   that we can just return immediately and we will reenter
	   recovery shortly again.
	   I.e. we deliberately leave the cluster with an inconsistent
	   generation id to allow us to abort recovery at any stage and
	   just restart it from scratch.
	 */
	vnnmap->generation = generation;
	ret = ctdb_ctrl_setvnnmap(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, vnnmap);
	if (ret != 0) {
		printf("Unable to set vnnmap for node %u\n", vnn);
		return -1;
	}


	/* set recovery mode to active on all nodes */
	ret = set_recovery_mode(ctdb, nodemap, CTDB_RECOVERY_ACTIVE);
	if (ret!=0) {
		printf("Unable to set recovery mode to active on cluster\n");
		return -1;
	}


	/* get a list of all databases */
	ret = ctdb_ctrl_getdbmap(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, &dbmap);
	if (ret != 0) {
		printf("Unable to get dbids from node :%d\n", vnn);
		return -1;
	}



	/* verify that all other nodes have all our databases */
	ret = create_missing_remote_databases(ctdb, nodemap, vnn, dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to create missing remote databases\n");
		return -1;
	}



	/* verify that we have all the databases any other node has */
	ret = create_missing_local_databases(ctdb, nodemap, vnn, &dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to create missing local databases\n");
		return -1;
	}



	/* verify that all other nodes have all our databases */
	ret = create_missing_remote_databases(ctdb, nodemap, vnn, dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to create missing remote databases\n");
		return -1;
	}



	/* pull all remote databases onto the local node */
	ret = pull_all_remote_databases(ctdb, nodemap, vnn, dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to pull remote databases\n");
		return -1;
	}



	/* repoint all local and remote database records to the local
	   node as being dmaster
	 */
	ret = update_dmaster_on_all_databases(ctdb, nodemap, vnn, dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to update dmaster on all databases\n");
		return -1;
	}



	/* push all local databases to the remote nodes */
	ret = push_all_local_databases(ctdb, nodemap, vnn, dbmap, mem_ctx);
	if (ret != 0) {
		printf("Unable to push local databases\n");
		return -1;
	}



	/* build a new vnn map */
	vnnmap = talloc_zero_size(mem_ctx, offsetof(struct ctdb_vnn_map, map) + 4*num_active);
	if (vnnmap == NULL) {
		DEBUG(0,(__location__ " Unable to allocate vnn_map structure\n"));
		exit(1);
	}
	vnnmap->generation = generation;
	vnnmap->size = num_active;
	for (i=j=0;i<nodemap->num;i++) {
		if (nodemap->nodes[i].flags&NODE_FLAGS_CONNECTED) {
			vnnmap->map[j++]=nodemap->nodes[i].vnn;
		}
	}


	/* push the new vnn map out to all the nodes */
	for (j=0; j<nodemap->num; j++) {
		/* dont push to nodes that are unavailable */
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}

		ret = ctdb_ctrl_setvnnmap(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, vnnmap);
		if (ret != 0) {
			printf("Unable to set vnnmap for node %u\n", vnn);
			return -1;
		}
	}


	/* disable recovery mode */
	ret = set_recovery_mode(ctdb, nodemap, CTDB_RECOVERY_NORMAL);
	if (ret!=0) {
		printf("Unable to set recovery mode to normal on cluster\n");
		return -1;
	}


	return 0;
}

void recoverd(struct ctdb_context *ctdb, struct event_context *ev)
{
	uint32_t vnn, num_active;
	TALLOC_CTX *mem_ctx=NULL;
	struct ctdb_node_map *nodemap=NULL;
	struct ctdb_node_map *remote_nodemap=NULL;
	struct ctdb_vnn_map *vnnmap=NULL;
	struct ctdb_vnn_map *remote_vnnmap=NULL;
	int i, j, ret;
	
again:
	printf("check if we need to do recovery\n");
	if (mem_ctx) {
		talloc_free(mem_ctx);
		mem_ctx = NULL;
	}
	mem_ctx = talloc_new(ctdb);
	if (!mem_ctx) {
		DEBUG(0,("Failed to create temporary context\n"));
		exit(-1);
	}

	/* we only check for recovery once every second */
	timed_out = 0;
	event_add_timed(ctdb->ev, mem_ctx, timeval_current_ofs(1, 0), timeout_func, ctdb);
	while (!timed_out) {
		event_loop_once(ev);
	}


	/* get our vnn number */
	vnn = ctdb_get_vnn(ctdb);  

	/* get number of nodes */
	ret = ctdb_ctrl_getnodemap(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, &nodemap);
	if (ret != 0) {
		printf("Unable to get nodemap from node %u\n", vnn);
		goto again;
	}

	/* count how many active nodes there are */
	num_active = 0;
	for (i=0; i<nodemap->num; i++) {
		if (nodemap->nodes[i].flags&NODE_FLAGS_CONNECTED) {
			num_active++;
		}
	}


	/* get the nodemap for all active remote nodes and verify
	   they are the same as for this node
	 */
	for (j=0; j<nodemap->num; j++) {
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}
		if (nodemap->nodes[j].vnn == vnn) {
			continue;
		}

		ret = ctdb_ctrl_getnodemap(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, &remote_nodemap);
		if (ret != 0) {
			printf("Unable to get nodemap from remote node %u\n", nodemap->nodes[j].vnn);
			goto again;
		}

		/* if the nodes disagree on how many nodes there are
		   then this is a good reason to try recovery
		 */
		if (remote_nodemap->num != nodemap->num) {
			printf("Remote node:%d has different node count. %d vs %d of the local node\n", nodemap->nodes[j].vnn, remote_nodemap->num, nodemap->num);
			do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
			goto again;
		}

		/* if the nodes disagree on which nodes exist and are
		   active, then that is also a good reason to do recovery
		 */
		for (i=0;i<nodemap->num;i++) {
			if ((remote_nodemap->nodes[i].vnn != nodemap->nodes[i].vnn)
			||  (remote_nodemap->nodes[i].flags != nodemap->nodes[i].flags)) {
				printf("Remote node:%d has different nodemap.\n", nodemap->nodes[j].vnn);
				do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
				goto again;
			}
		}

	}

	/* get the vnnmap */
	ret = ctdb_ctrl_getvnnmap(ctdb, timeval_current_ofs(1, 0), vnn, mem_ctx, &vnnmap);
	if (ret != 0) {
		printf("Unable to get vnnmap from node %u\n", vnn);
		goto again;
	}

	/* there better be the same number of lmasters in the vnn map
	   as there are active nodes or well have to do a recovery
	 */
	if (vnnmap->size != num_active) {
		printf("The vnnmap count is different from the number of active nodes. %d vs %d\n", vnnmap->size, num_active);
		do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
		goto again;
	}

	/* verify that all active nodes in the nodemap also exist in 
	   the vnnmap.
	 */
	for (j=0; j<nodemap->num; j++) {
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}
		if (nodemap->nodes[j].vnn == vnn) {
			continue;
		}

		for (i=0; i<vnnmap->size; i++) {
			if (vnnmap->map[i] == nodemap->nodes[j].vnn) {
				break;
			}
		}
		if (i==vnnmap->size) {
			printf("Node %d is active in the nodemap but did not exist in the vnnmap\n", nodemap->nodes[j].vnn);
			do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
			goto again;
		}
	}

	
	/* verify that all other nodes have the same vnnmap
	   and are from the same generation
	 */
	for (j=0; j<nodemap->num; j++) {
		if (!(nodemap->nodes[j].flags&NODE_FLAGS_CONNECTED)) {
			continue;
		}
		if (nodemap->nodes[j].vnn == vnn) {
			continue;
		}

		ret = ctdb_ctrl_getvnnmap(ctdb, timeval_current_ofs(1, 0), nodemap->nodes[j].vnn, mem_ctx, &remote_vnnmap);
		if (ret != 0) {
			printf("Unable to get vnnmap from remote node %u\n", nodemap->nodes[j].vnn);
			goto again;
		}

		/* verify the vnnmap generation is the same */
		if (vnnmap->generation != remote_vnnmap->generation) {
			printf("Remote node %d has different generation of vnnmap. %d vs %d (ours)\n", nodemap->nodes[j].vnn, remote_vnnmap->generation, vnnmap->generation);
			do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
			goto again;
		}

		/* verify the vnnmap size is the same */
		if (vnnmap->size != remote_vnnmap->size) {
			printf("Remote node %d has different size of vnnmap. %d vs %d (ours)\n", nodemap->nodes[j].vnn, remote_vnnmap->size, vnnmap->size);
			do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
			goto again;
		}

		/* verify the vnnmap is the same */
		for (i=0;i<vnnmap->size;i++) {
			if (remote_vnnmap->map[i] != vnnmap->map[i]) {
				printf("Remote node %d has different vnnmap.\n", nodemap->nodes[j].vnn);
				do_recovery(ctdb, ev, mem_ctx, vnn, num_active, nodemap, vnnmap);
				goto again;
			}
		}
	}

	printf("no we did not need to do recovery\n");
	goto again;

}

/*
  main program
*/
int main(int argc, const char *argv[])
{
	struct ctdb_context *ctdb;
	struct poptOption popt_options[] = {
		POPT_AUTOHELP
		POPT_CTDB_CMDLINE
		POPT_TABLEEND
	};
	int opt;
	const char **extra_argv;
	int extra_argc = 0;
	int ret;
	poptContext pc;
	struct event_context *ev;

	pc = poptGetContext(argv[0], argc, argv, popt_options, POPT_CONTEXT_KEEP_FIRST);

	while ((opt = poptGetNextOpt(pc)) != -1) {
		switch (opt) {
		default:
			fprintf(stderr, "Invalid option %s: %s\n", 
				poptBadOption(pc, 0), poptStrerror(opt));
			exit(1);
		}
	}

	/* setup the remaining options for the main program to use */
	extra_argv = poptGetArgs(pc);
	if (extra_argv) {
		extra_argv++;
		while (extra_argv[extra_argc]) extra_argc++;
	}

#if 0
	if (extra_argc < 1) {
		usage();
	}
#endif

	ev = event_context_init(NULL);

	/* initialise ctdb */
	ctdb = ctdb_cmdline_client(ev);
	if (ctdb == NULL) {
		printf("Failed to init ctdb\n");
		exit(1);
	}


	recoverd(ctdb, ev);

	return ret;
}
