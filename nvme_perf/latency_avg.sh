perf_exe="/home/akalia/sandbox/spdk/examples/nvme/perf/perf"

rm -f tmpout_*
rm -rf final_out
touch final_out

# Last one wins
bench=read       # Sequential reads
bench=write      # Sequential writes
bench=randwrite  # Random writes

for ((size = 512; size <= 65536; size *= 2)); do
  tmpfile="tmpout_$size"

  # -q: queue depth
  # -o: object size to write
  # -t: time in seconds
  # -c: core mask (core 24)
  # -L: generate histogram
  sudo numactl --cpunodebind=1 --membind=1 $perf_exe \
    -q 1 -o $size -w $bench -t 2 -c 0x1000000 > $tmpfile

  us_avg=`cat $tmpfile  | grep Total | sed -s 's/  */ /g' | cut -d ' ' -f 5`

  echo $size $us_avg
  echo $size $us_avg >> final_out
done

cat final_out
rm -f tmpout_*
rm -rf final_out
