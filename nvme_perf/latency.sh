perf_exe="/home/akalia/sandbox/spdk/examples/nvme/perf/perf"

rm -f tmpout_*
rm -rf final_out
touch final_out

# Last one wins
bench=read       # Sequential reads
bench=randwrite  # Random writes
bench=write      # Sequential writes
bench=randread   # Random reads

echo "size us_avg us_median us_999 us_99" >> final_out

for ((size = 512; size <= 65536; size *= 2)); do
  tmpfile="tmpout_$size"

  # -q: queue depth
  # -o: object size to write
  # -t: time in seconds
  # -c: core mask (core 24)
  # -L: generate histogram
  sudo numactl --cpunodebind=1 --membind=1 $perf_exe \
    -q 1 -o $size -w $bench -t 2 -c 0x1000000 -L > $tmpfile

  us_avg=`cat $tmpfile  | grep Total | sed -s 's/  */ /g' | cut -d ' ' -f 5`
  us_median=`cat $tmpfile  | grep "50\.00000"  | tr -d ' ' |  cut -d ":" -f 2 | sed 's/us//g'`
  us_99=`cat $tmpfile  | grep "99\.00000"  | tr -d ' ' |  cut -d ":" -f 2 | sed 's/us//g'`
  us_999=`cat $tmpfile  | grep "99\.90000"  | tr -d ' ' |  cut -d ":" -f 2 | sed 's/us//g'`

  echo $size $us_avg $us_median $us_999 $us_99
  echo $size $us_avg $us_median $us_999 $us_99 >> final_out
done

cat final_out
rm -f tmpout_*
rm -rf final_out
