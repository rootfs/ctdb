#!/bin/sh

. "${TEST_SCRIPTS_DIR}/unit.sh"

define_test "takeip, monitor -> reconfigure, replay error"

setup_nfs

public_address=$(ctdb_get_1_public_address)

err="foo: bar error occurred"

ok_null

simple_test_event "takeip" $public_address

ctdb_fake_scriptstatus 1 "ERROR" "$err"

required_result 1 <<EOF
Reconfiguring service "nfs"...
Replaying previous status for this script due to reconfigure...
$err
EOF

simple_test_event "monitor"
