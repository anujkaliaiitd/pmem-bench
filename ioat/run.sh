size=50000
batch_size=8
flush_cachelines=1
use_ioat=1

stat_file=$(mktemp)
out_file=$(mktemp)

function sweep_num_ioat_engines() {
  window_sizes="1 2 4 8 64"
  echo "size $window_sizes" > ${stat_file} # Stats file header

  for size in 1000 2000 3000 4000 5000 50000 500000; do
    stat_str="$size" # Saved in stat_file at the end of a window
    for window_size in $window_sizes; do 
      sudo -E env numactl --physcpubind=0 --membind=0 ./bench \
        --use_ioat $use_ioat \
        --size $size \
        --window_size $window_size \
        --flush_cachelines $flush_cachelines 1>${out_file} 2>${out_file}

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
