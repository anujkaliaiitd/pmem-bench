for num_counters in `seq 1 32`; do
  make 1>/dev/null 2>/dev/null
  t=`/usr/bin/time -f "%e" numactl --physcpubind=3 --membind=0 ./bench $num_counters`

  #echo "$kNumCounters;$t"
done
