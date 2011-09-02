#!/bin/sh

. "${EVENTSCRIPTS_TESTS_DIR}/common.sh"

define_test "takeip, ipreallocated -> reconfigure"

setup_nfs

public_address=$(ctdb_get_1_public_address)

ok_null

simple_test_event "takeip" $public_address

ok <<EOF
Reconfiguring service "nfs"...
Starting nfslock: OK
Starting nfs: OK
EOF

simple_test_event "ipreallocated"