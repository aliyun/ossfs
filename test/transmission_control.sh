#!/bin/bash

IFACE="eth0"
# The rate may not be restricted to specifically 100mbit
# Adjust this value according to the practical 
RATE="16mbit"

modprobe ifb numifbs=1
ip link set dev ifb0 up

tc qdisc del dev $IFACE ingress 2>/dev/null
tc qdisc del dev ifb0 root 2>/dev/null

tc qdisc add dev $IFACE ingress
tc filter add dev $IFACE parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0

tc qdisc add dev ifb0 root handle 1: htb default 30
tc class add dev ifb0 parent 1: classid 1:1 htb rate $RATE ceil $RATE
tc class add dev ifb0 parent 1:1 classid 1:30 htb rate $RATE ceil $RATE
