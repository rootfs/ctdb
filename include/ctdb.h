/*
   ctdb database library

   Copyright (C) Ronnie sahlberg 2010
   Copyright (C) Rusty Russell 2010

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

#ifndef _CTDB_H
#define _CTDB_H
#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <tdb.h>
#include <ctdb_protocol.h>

/* The type of the first arg should match the arg given to ctdb_connect() */
typedef void (*ctdb_log_fn_t)(void *log_priv,
			      int severity, const char *format, va_list ap);


/* All *_send() functions are guaranteed to be non-blocking and fully
 * asynchronous.  The non-_send variants are synchronous.
 */

/*
 * Connect to ctdb using the specified domain socket.
 * Returns a ctdb context if successful or NULL.
 *
 */
struct ctdb_connection *ctdb_connect(const char *addr,
				     ctdb_log_fn_t log_fn, void *log_priv);

int ctdb_get_fd(struct ctdb_connection *ctdb);

int ctdb_which_events(struct ctdb_connection *ctdb);

bool ctdb_service(struct ctdb_connection *ctdb, int revents);

struct ctdb_request;

void ctdb_request_free(struct ctdb_connection *ctdb, struct ctdb_request *req);

/*
 * Callback for completed requests: it would normally unpack the request
 * using ctdb_*_recv().
 * You must free the request using ctdb_request_free().
 *
 * Note that due to macro magic, your callback doesn't have to take void *,
 * it can take a type which matches the actual private parameter.
 */
typedef void (*ctdb_callback_t)(struct ctdb_connection *ctdb,
				struct ctdb_request *req, void *private);


/*
 * functions to attach to a database
 * if the database does not exist it will be created.
 *
 * You have to free the handle with ctdb_detach_db() when finished with it.
 */
struct ctdb_db;

struct ctdb_request *
ctdb_attachdb_send(struct ctdb_connection *ctdb,
		   const char *name, int persistent, uint32_t tdb_flags,
		   ctdb_callback_t callback, void *private_data);

struct ctdb_db *ctdb_attachdb_recv(struct ctdb_connection *ctdb,
				   struct ctdb_request *req);

struct ctdb_db *ctdb_attachdb(struct ctdb_connection *ctdb,
			      const char *name, int persistent,
			      uint32_t tdb_flags);

struct ctdb_lock;

/*
 * functions to read a record from the database
 * when the callback is invoked, the client will hold an exclusive lock
 * on the record, the client MUST NOT block during holding this lock and MUST
 * release it quickly by performing ctdb_release_lock(lock).
 *
 * When the lock is released, data is freed too, so make sure to copy the data
 * before that.
 *
 * This returns true on success: the callback may have already been called,
 * or it might be awaiting a response from ctdbd.
 */
typedef void (*ctdb_rrl_callback_t)(struct ctdb_db *ctdb_db,
				    struct ctdb_lock *lock,
				    TDB_DATA data,
				    void *private);
bool
ctdb_readrecordlock_async(struct ctdb_db *ctdb_db, TDB_DATA key,
			  ctdb_rrl_callback_t callback, void *private_data);

/* Returns null on failure. */
struct ctdb_lock *ctdb_readrecordlock(struct ctdb_db *ctdb_db, TDB_DATA key,
				      TDB_DATA *data);

/*
 * Function to write data to a record
 * This function may ONLY be called while holding a lock to the record
 * created by ctdb_readrecordlock*, and before calling
 * ctdb_release_lock() to release the lock.
 */
int ctdb_writerecord(struct ctdb_lock *lock, TDB_DATA data);


void ctdb_release_lock(struct ctdb_lock *lock);

/*
 * messaging functions
 * these functions provide a messaging layer for applications to communicate
 * with eachother across
 */
typedef void (*ctdb_message_fn_t)(struct ctdb_connection *, uint64_t srvid, TDB_DATA data, void *);

struct ctdb_request *
ctdb_set_message_handler_send(struct ctdb_connection *ctdb, uint64_t srvid,
			      ctdb_message_fn_t handler,
			      ctdb_callback_t callback,
			      void *private_data);

bool ctdb_set_message_handler_recv(struct ctdb_connection *ctdb,
				   struct ctdb_request *handle);

bool ctdb_set_message_handler(struct ctdb_connection *ctdb, uint64_t srvid,
			      ctdb_message_fn_t handler, void *private_data);



/*
 * unregister a message handler and stop listening on teh specified port
 */
struct ctdb_request *
ctdb_remove_message_handler_send(struct ctdb_connection *ctdb, uint64_t srvid,
				 ctdb_callback_t callback,
				 void *private_data);

bool ctdb_remove_message_handler_recv(struct ctdb_request *handle);

bool ctdb_remove_message_handler(struct ctdb_connection *ctdb, uint64_t srvid);



/*
 * send a message to a specific node/port
 * this function is non-blocking
 */
bool ctdb_send_message(struct ctdb_connection *ctdb, uint32_t pnn, uint64_t srvid, TDB_DATA data);



/*
 * functions to read the pnn number of the local node
 */
struct ctdb_request *
ctdb_getpnn_send(struct ctdb_connection *ctdb,
		 uint32_t destnode,
		 ctdb_callback_t callback,
		 void *private_data);
bool ctdb_getpnn_recv(struct ctdb_connection *ctdb,
		      struct ctdb_request *req, uint32_t *pnn);

bool ctdb_getpnn(struct ctdb_connection *ctdb,
		 uint32_t destnode,
		 uint32_t *pnn);




/*
 * functions to read the recovery master of a node
 */
struct ctdb_request *
ctdb_getrecmaster_send(struct ctdb_connection *ctdb,
			uint32_t destnode,
			ctdb_callback_t callback,
			void *private_data);
bool ctdb_getrecmaster_recv(struct ctdb_connection *ctdb,
			    struct ctdb_request *handle,
			    uint32_t *recmaster);
bool ctdb_getrecmaster(struct ctdb_connection *ctdb,
		       uint32_t destnode,
		       uint32_t *recmaster);




/*
 * cancel a request
 */
void ctdb_cancel(struct ctdb_connection *ctdb, struct ctdb_request *req);


/*
 * functions for logging errors
 */
extern int ctdb_log_level; /* LOG_WARNING and above by default. */
void ctdb_log_file(FILE *, int severity, const char *format, va_list ap);


/* These ugly macro wrappers make the callbacks typesafe. */
#include <ctdb_typesafe_cb.h>
#define ctdb_sendcb(cb, cbdata)						\
	 typesafe_cb_preargs(void, (cb), (cbdata),			\
			     struct ctdb_connection *, struct ctdb_request *)

#define ctdb_connect(addr, log, logpriv)				\
	ctdb_connect((addr),						\
		     typesafe_cb_postargs(void, (log), (logpriv),	\
					  int, const char *, va_list),	\
		     (logpriv))


#define ctdb_attachdb_send(ctdb, name, persistent, tdb_flags, cb, cbdata) \
	ctdb_attachdb_send((ctdb), (name), (persistent), (tdb_flags),	\
			   ctdb_sendcb((cb), (cbdata)), (cbdata))

#define ctdb_readrecordlock_async(_ctdb_db, key, cb, cbdata)		\
	ctdb_readrecordlock_async((_ctdb_db), (key),			\
		typesafe_cb_preargs(void, (cb), (cbdata),		\
				    struct ctdb_db *, struct ctdb_lock *, \
				    TDB_DATA), (cbdata))

#define ctdb_set_message_handler_send(ctdb, srvid, handler, cb, cbdata)	\
	ctdb_set_message_handler_send((ctdb), (srvid), (handler),	\
	      ctdb_sendcb((cb), (cbdata)), (cbdata))

#define ctdb_remove_message_handler_send(ctdb, srvid, cb, cbdata)	\
	ctdb_remove_message_handler_send((ctdb), (srvid),		\
	      ctdb_sendcb((cb), (cbdata)), (cbdata))

#define ctdb_getpnn_send(ctdb, destnode, cb, cbdata)			\
	ctdb_getpnn_send((ctdb), (destnode),				\
			 ctdb_sendcb((cb), (cbdata)), (cbdata))

#define ctdb_getrecmaster_send(ctdb, destnode, cb, cbdata)		\
	ctdb_getrecmaster_send((ctdb), (destnode),			\
			       ctdb_sendcb((cb), (cbdata)), (cbdata))
#endif
