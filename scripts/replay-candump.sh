#!/bin/bash

CAN_IFACE=can4
VIRT_IFACE=vcan0
LOG_FILE=logs/toyota_can_highway.log

if [ -n "$1" ]
then
    CAN_IFACE=$1
fi

if [ -n "$2" ]
then
    VIRT_IFACE=$2
fi

if [ -n "$3" ]
then
    LOG_FILE=$3
fi

canplayer "${VIRT_IFACE}"="${CAN_IFACE}" -v -I "${LOG_FILE}" -l i -g 1 &

trap "pkill canplayer" SIGINT

wait
