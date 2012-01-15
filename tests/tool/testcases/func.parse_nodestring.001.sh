#!/bin/sh

. "${TESTS_SUBDIR}/common.sh"

define_test "all, dd_ok, 3 healthy"

required_result <<EOF
NODES: 0 1 2
PNN MODE: BROADCAST_ALL (4026531842)
EOF

simple_test all true <<EOF
0       192.168.20.41   0x0
1       192.168.20.42   0x0
2       192.168.20.43   0x0     CURRENT RECMASTER
EOF
