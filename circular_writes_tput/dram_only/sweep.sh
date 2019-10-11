for kNumCounters in `seq 1 32`; do
  rm config.h
  touch config.h

  echo "static constexpr size_t kNumCounters = $kNumCounters;" >> config.h

  make 1>/dev/null 2>/dev/null
  t=`/usr/bin/time -f "%e" numactl --physcpubind=3 --membind=0 ./bench`

  #echo "$kNumCounters;$t"
done
