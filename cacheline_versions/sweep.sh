for kNumCounters in 1 2 3 4 5 8 16; do
  for kStrideSize in 256 4096; do
    rm config.h
    touch config.h

    echo "static constexpr size_t kNumCounters = $kNumCounters;" >> config.h
    echo "static constexpr size_t kStrideSize = $kStrideSize;" >> config.h

    make
    ./run.sh
  done
done
