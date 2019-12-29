for num_counters in 1 2 3 4 5 8 16; do
  for stride_size in 64 256; do
    rm config.h
    touch config.h

    sudo -E env numactl --physcpubind=3 --membind=0 ./bench \
      --num_counters=$num_counters \
      --stride_size=$stride_size
  done
done
