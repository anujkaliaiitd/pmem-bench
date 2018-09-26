batch_size=16
benchmark=set

for ((num_keys = (64 * 1024 * 1024); num_keys <= (64 * 1024 * 1024); num_keys *= 2)); do
  numactl --physcpubind=3 --membind=0 ./bench \
    --table_key_capacity $num_keys \
    --batch_size $batch_size \
    --benchmark $benchmark
done
