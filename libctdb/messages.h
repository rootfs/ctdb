#ifndef _LIBCTDB_MESSAGE_H
#define _LIBCTDB_MESSAGE_H
struct message_handler_info;
struct ctdb_connection;
struct ctdb_req_header;

void deliver_message(struct ctdb_connection *ctdb, struct ctdb_req_header *hdr);
#endif /* _LIBCTDB_MESSAGE_H */
