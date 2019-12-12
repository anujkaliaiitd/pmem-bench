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
#  * This script uses `printf` to convert hex to decimal, and `xargs` to
#    trim surrounding whitespaces.
#  * During idle period, the NVM controller may write to NVM even when no DDR
#    commands are received. This causes write amplification to be ~100.
#

dimms=0x0001 # Single non-interleaved
dimms=0x0001,0x0011,0x0021,0x0101,0x0111,0x0121  # All DIMMs at socket 0

# Sum metric arg #1 from file watch_out
sum_from_watch_out() {
  temp_file=$(mktemp)
  cat watch_out | grep $1 | cut -d'=' -f 2 > $temp_file

  sum=0
  while read hex; do
    dec=`printf "%d\n" $hex`
    sum=`expr $sum + $dec`
  done < ${temp_file}

  echo $sum
  rm ${temp_file}
}

# Regenerate watch_out
refresh_watch_out() {
  rm -f watch_out
  touch watch_out
  sudo ipmctl show -dimm $dimms -performance MediaWrites,WriteRequests,MediaReads,ReadRequests > watch_out
}

refresh_watch_out
media_writes_0=`sum_from_watch_out MediaWrites`
ddr_writes_0=`sum_from_watch_out WriteRequests`
media_reads_0=`sum_from_watch_out MediaReads`
ddr_reads_0=`sum_from_watch_out ReadRequests`

sleep_seconds=1
while true; do
  sleep $sleep_seconds

  refresh_watch_out
  media_writes_1=`sum_from_watch_out MediaWrites`
  ddr_writes_1=`sum_from_watch_out WriteRequests`
  media_reads_1=`sum_from_watch_out MediaReads`
  ddr_reads_1=`sum_from_watch_out ReadRequests`

  media_writes_delta=`calc $media_writes_1 - $media_writes_0 | xargs`
  ddr_writes_delta=`calc $ddr_writes_1 - $ddr_writes_0 | xargs`
  media_reads_delta=`calc $media_reads_1 - $media_reads_0 | xargs`
  ddr_reads_delta=`calc $ddr_reads_1 - $ddr_reads_0 | xargs`

  media_writes_GBs=`python -c "print $media_writes_delta * 64.0 / (1024 * 1024 * 1024 * $sleep_seconds)" | xargs`
  ddr_writes_GBs=`python -c "print $ddr_writes_delta * 64.0 / (1024 * 1024 * 1024 * $sleep_seconds)" | xargs`
  media_reads_GBs=`python -c "print $media_reads_delta * 64.0 / (1024 * 1024 * 1024 * $sleep_seconds)" | xargs`
  ddr_reads_GBs=`python -c "print $ddr_reads_delta * 64.0 / (1024 * 1024 * 1024 * $sleep_seconds)" | xargs`
  write_amp=`calc $media_writes_delta / $ddr_writes_delta | xargs`
  read_amp=`calc $media_reads_delta / $ddr_reads_delta | xargs`

  echo "Media writes = $media_writes_delta ($media_writes_GBs GB/s), DDR writes = $ddr_writes_delta ($ddr_writes_GBs GB/s), amplification = $write_amp"
  echo "Media reads = $media_reads_delta ($media_reads_GBs GB/s), DDR reads = $ddr_reads_delta ($ddr_reads_GBs GB/s), amplification = $read_amp"
  echo ""

  media_writes_0=$media_writes_1
  ddr_writes_0=$ddr_writes_1
  media_reads_0=$media_reads_1
  ddr_reads_0=$ddr_reads_1
done
