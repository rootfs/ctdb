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

static void send_result(int fd, char result)
{
	write(fd, &result, 1);
	if (result == 1) {
		exit(1);
	}
}


int main(int argc, char *argv[])
{
	TDB_DATA key;
	const char *dbpath, *dbkey;
	int write_fd;
	struct tdb_context *tdb;
	char result = 1;
	int ppid;

	if (argc != 5) {
		fprintf(stderr, "Usage: %s <ctdbd-pid> <output-fd> <db-path> <db-key>\n", argv[0]);
		exit(1);
	}

	ppid = atoi(argv[1]);
	write_fd = atoi(argv[2]);
	dbpath = argv[3];
	dbkey = argv[4];

	/* Convert hex key to key */
	if (strcmp(dbkey, "NULL") == 0) {
		key.dptr = NULL;
		key.dsize = 0;
	} else {
		key.dptr = hex_decode_talloc(NULL, dbkey, &key.dsize);
	}

	tdb = tdb_open(dbpath, 0, TDB_DEFAULT, O_RDWR, 0600);
	if (tdb == NULL) {
		fprintf(stderr, "Lockwait: Error opening database %s\n", dbpath);
		send_result(write_fd, result);
	}

	if (tdb_chainlock(tdb, key) < 0) {
		fprintf(stderr, "Lockwait: Error locking (%s)\n", tdb_errorstr(tdb));
		send_result(write_fd, result);
	}

	result = 0;
	send_result(write_fd, result);

	while (kill(ppid, 0) == 0 || errno != ESRCH) {
		sleep(5);
	}
	return 0;
}
