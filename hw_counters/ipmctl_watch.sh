#!/bin/bash
#
# README:
#
# This script monitors write amplification for DIMM 0 using ipmctl
#
# MediaWrites = Number of 64-byte writes to NVM. The NVM controller issues
# 256-byte writes internally, but ipmctl multiplies it by four
# 
# WriteRequests = Number of 64-byte write requests received on the DDR bus
#
# Notes:
#  * This script uses `calc` to convert hex to decimal, and `xargs` to
#    trim surrounding whitespaces.
#  * During idle period, the NVM controller may write to NVM even when no DDR
#    commands are received. This causes write amplification to be ~100.

media_writes_0=`sudo ipmctl show -dimm 0x0001 -performance MediaWrites \
  | grep MediaWrites | cut -d'=' -f 2`

ddr_writes_0=`sudo ipmctl show -dimm 0x0001 -performance WriteRequests \
  | grep WriteRequests | cut -d'=' -f 2`

while true; do

  media_writes_1=`sudo ipmctl show -dimm 0x0001 -performance MediaWrites \
    | grep MediaWrites | cut -d'=' -f 2`

  ddr_writes_1=`sudo ipmctl show -dimm 0x0001 -performance WriteRequests \
    | grep WriteRequests | cut -d'=' -f 2`

  media_writes_delta=`calc $media_writes_1 - $media_writes_0 | xargs`
  ddr_writes_delta=`calc $ddr_writes_1 - $ddr_writes_0 | xargs`
  amp=`calc $media_writes_delta / $ddr_writes_delta | xargs`

  echo "Media writes = $media_writes_delta, DDR writes = $ddr_writes_delta, amplification = $amp"

  media_writes_0=$media_writes_1
  ddr_writes_0=$ddr_writes_1

  sleep 1
done

