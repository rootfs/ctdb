/*
   ctdb lockwait helper

   Copyright (C) Amitay Isaacs  2013

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
#include "tdb.h"
#include "system/filesys.h"
#include "../include/ctdb_private.h"

/*
 * Use stdin to read information about record and database
 *
 * 1. Database full path
 * 2. Key
 */

struct lock_info {
	char *dbpath;
	TDB_DATA dbkey;
};

static bool read_data(int fd, TDB_DATA *data)
{
	int len;

	if (read(fd, &len, sizeof(len)) != sizeof(len)) {
		return false;
	}

	data->dptr = malloc(len);
	if (data->dptr == NULL) {
		return false;
	}
	data->dsize = len;

	if (read(fd, data->dptr, len) != len) {
		free(data->dptr);
		return false;
	}

	return true;
}

/* This is a copy of ctdb_marshall_loop_next() */
static struct ctdb_rec_data *extract_key_data(struct ctdb_marshall_buffer *m,
					      struct ctdb_rec_data *r,
					      TDB_DATA *key, TDB_DATA *data)
{
	if (r == NULL) {
		r = (struct ctdb_rec_data *)&m->data[0];
	} else {
		r = (struct ctdb_rec_data *)(r->length + (uint8_t *)r);
	}

	key->dptr = &r->data[0];
	key->dsize = r->keylen;

	data->dptr = &r->data[r->keylen];
	data->dsize = r->datalen;

	return r;
}


static bool parse_data(TDB_DATA buffer, struct lock_info *lock)
{
	struct ctdb_marshall_buffer *m = (struct ctdb_marshall_buffer *)buffer.dptr;
	struct ctdb_rec_data *r;
	TDB_DATA key, data;

	/* Database name */
	r = extract_key_data(m, NULL, &key, &data);
	if (key.dsize == 0 || data.dsize == 0) {
		return false;
	}
	if (strncmp((char *)key.dptr, "dbpath", 6) != 0) {
		return false;
	}
	lock->dbpath = strndup((char *)data.dptr, data.dsize);
	if (lock->dbpath == NULL) {
		return false;
	}

	/* Database key */
	if (r == NULL) {
		return false;
	}
	r = extract_key_data(m, r, &key, &data);
	if (key.dsize == 0 || data.dsize == 0) {
		return false;
	}

	if (strncmp((char *)key.dptr, "dbkey", 5) != 0) {
		return false;
	}
	lock->dbkey.dptr = malloc(data.dsize);
	if (lock->dbkey.dptr == NULL) {
		return false;
	}
	lock->dbkey.dsize = data.dsize;
	memcpy(lock->dbkey.dptr, data.dptr, data.dsize);

	return true;
}


static void send_result(int fd, char result)
{
	write(fd, &result, 1);
	if (result == 1) {
		exit(1);
	}
}


int main(int argc, char *argv[])
{
	TDB_DATA data;
	struct lock_info lock;
	struct tdb_context *tdb;
	char result = 1;
	int read_fd, write_fd;
	int hash_size;
	int ppid;

	if (argc != 4) {
		fprintf(stderr, "Usage: %s <read-fd> <write-fd> <db-hashsize>\n", argv[0]);
		exit(1);
	}

	read_fd = atoi(argv[1]);
	write_fd = atoi(argv[2]);
	hash_size = atoi(argv[3]);

	if (!read_data(read_fd, &data)) {
		send_result(write_fd, result);
	}

	if (!parse_data(data, &lock)) {
		send_result(write_fd, result);
	}

	tdb = tdb_open(lock.dbpath, hash_size, TDB_DEFAULT, O_RDWR, 0600);
	if (tdb == NULL) {
		fprintf(stderr, "Error opening database %s\n", lock.dbpath);
		send_result(write_fd, result);
	}

	if (tdb_chainlock(tdb, lock.dbkey) < 0) {
		fprintf(stderr, "Error locking (%s)\n", tdb_errorstr(tdb));
		send_result(write_fd, result);
	}

	result = 0;
	send_result(write_fd, result);

	ppid = getppid();
	while (kill(ppid, 0) == 0 || errno != ESRCH) {
		sleep(5);
	}
	return 0;
}
