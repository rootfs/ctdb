/* 
   simple rb test tool

   Copyright (C) Ronnie Sahlberg 2007

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

#include "includes.h"
#include "lib/events/events.h"
#include "lib/util/dlinklist.h"
#include "system/filesys.h"
#include "popt.h"
#include "cmdline.h"

#include <sys/time.h>
#include <time.h>
#include "common/rb_tree.h"

int num_records;

void *callback(void *param, void *d)
{
	uint32_t *data = (uint32_t *)d;

	if(!data){
		data = talloc(NULL, uint32_t);
		*data = 0;
	}
	(*data)++;

	return data;
}
	
				
/*
  main program
*/
int main(int argc, const char *argv[])
{
	struct poptOption popt_options[] = {
		POPT_AUTOHELP
		POPT_CTDB_CMDLINE
		{ "num-records", 'r', POPT_ARG_INT, &num_records, 0, "num_records", "integer" },
		POPT_TABLEEND
	};
	int opt;
	const char **extra_argv;
	int extra_argc = 0;
	poptContext pc;
	struct event_context *ev;
	int i;
	trbt_tree_t *tree;

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

	ev = event_context_init(NULL);


	printf("testing trbt_insert32_callback for %d records\n", num_records);
	tree = trbt_create(NULL);
	for (i=0; i<num_records; i++) {
		trbt_insert32_callback(tree, i, callback, NULL);
	}
	for (i=3; i<num_records; i++) {
		trbt_insert32_callback(tree, i, callback, NULL);
	}

	printf("first 3 keys should have data==1\n");
	printf("the rest of the keys should have data==2\n");
	for (i=0; i<num_records; i++) {
		uint32_t *data;

		data = trbt_lookup32(tree, i);
		printf("key:%d data:%d\n", i, *data);
	}



	return 0;
}
