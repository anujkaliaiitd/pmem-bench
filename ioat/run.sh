#!/bin/bash

num_prints=3
use_ioat=1
use_pmem=1
numa_node=1 

stat_file=$(mktemp)
out_file=$(mktemp)

function sweep_num_ioat_engines() {
  window_sizes="1 8"
  echo "size $window_sizes" > ${stat_file} # Stats file header

  for size in 1024 2048 4096 8192 16384 32768 65536 131072; do
    stat_str="$size" # Saved in stat_file at the end of a window
    for window_size in $window_sizes; do 
      sudo -E env numactl --cpunodebind=$numa_node --membind=$numa_node ./bench \
        --num_prints $num_prints \
        --use_ioat $use_ioat \
        --use_pmem $use_pmem \
        --numa_node $numa_node \
        --size $size \
        --window_size $window_size 1>${out_file} 2>${out_file}

      # The last num_prints lines of out_file are formatted like:
      # 10.2 GB/s
      avg=`cat ${out_file} | tail -$num_prints | cut -d' ' -f 1 | avg.awk`
      echo "size $size, window size $window_size, tput $avg GB/s"

      stat_str="$stat_str $avg"
    done

    echo "Saving $stat_str to ${stat_file}"
    echo $stat_str >> ${stat_file}
  done

  echo "Results for: use_ioat $use_ioat, use_pmem $use_pmem"
  cat ${stat_file}
}

sweep_num_ioat_engines
