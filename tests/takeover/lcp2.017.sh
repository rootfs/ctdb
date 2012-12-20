#!/bin/sh

. "${TEST_SCRIPTS_DIR}/unit.sh"

define_test "3 nodes, no IPs assigned, all unhealthy, NoIPTakeoverOnDisabled"

export CTDB_TEST_LOGLEVEL=0
export CTDB_SET_NoIPTakeoverOnDisabled=1

required_result <<EOF
192.168.21.254 -1
192.168.21.253 -1
192.168.21.252 -1
192.168.20.254 -1
192.168.20.253 -1
192.168.20.252 -1
192.168.20.251 -1
192.168.20.250 -1
192.168.20.249 -1
EOF

simple_test 2,2,2 <<EOF
192.168.21.254 -1
192.168.21.253 -1
192.168.21.252 -1
192.168.20.254 -1
192.168.20.253 -1
192.168.20.252 -1
192.168.20.251 -1
192.168.20.250 -1
192.168.20.249 -1
EOF
