num_threads=48
numactl --cpunodebind=0 --membind=0 ./bench --num_threads=$num_threads
