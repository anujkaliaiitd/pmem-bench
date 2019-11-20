stat_file=$(mktemp)
out_file=$(mktemp)

function sweep_num_ioat_engines() {
  window_sizes="8"
  echo "size $window_sizes" > ${stat_file} # Stats file header

  for size in `seq 4096 16 4352`; do
    stat_str="$size" # Saved in stat_file at the end of a window
    for window_size in $window_sizes; do 
      echo "size $size, window size $window_size"
      sudo -E env numactl --physcpubind=0 --membind=0 ./bench \
        --size $size \
        --window_size $window_size 2>&1 | tee ${out_file}

      # The last five lines of out_file are formatted like:
      # 10.2 GB/s
      avg=`cat ${out_file} | tail -5 | cut -d' ' -f 1 | avg.awk`
      echo "size $size, window size $window_size, tput $avg GB/s"

      stat_str="$stat_str $avg"
      sleep 1
    done

    echo "Saving $stat_str to ${stat_file}"
    echo $stat_str >> ${stat_file}
  done

  cat ${stat_file}
}

sweep_num_ioat_engines
