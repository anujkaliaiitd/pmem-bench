for kWriteSize in 32 64 128; do
  for kBufferSize in 128 256 512; do
    for kNumBuffers in 2 4 8 16 32; do
      rm config.h
      touch config.h

      echo "static constexpr size_t kWriteSize = $kWriteSize;" >> config.h
      echo "static constexpr size_t kBufferSize = $kBufferSize;" >> config.h
      echo "static constexpr size_t kNumBuffers = $kNumBuffers;" >> config.h

      make
      ./run.sh
    done
  done
done
