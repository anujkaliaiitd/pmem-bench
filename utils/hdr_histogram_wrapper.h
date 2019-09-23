#include <hdr/hdr_histogram.h>

// A wrapper for hdr_histogram that supports floating point values with
// magnified precision. A floating point record x is inserted as x * AMP.
template <size_t AMP>
class HdrHistogramAmp {
 public:
  HdrHistogramAmp(int64_t min, int64_t max, uint32_t precision) {
    int ret = hdr_init(min * AMP, max * AMP, precision, &hist);
    rt_assert(ret == 0);
  }

  ~HdrHistogramAmp() { hdr_close(hist); }

  inline void record_value(double v) { hdr_record_value(hist, v * AMP); }

  double percentile(double p) {
    return hdr_value_at_percentile(hist, p) / (AMP * 1.0);
  }

  void reset() { hdr_reset(hist); }

  hdr_histogram *get_raw_hist() { return hist; }

 private:
  hdr_histogram *hist = nullptr;
};

// A conveinince wrapper for hdr_histogram
class HdrHistogram {
 public:
  HdrHistogram(int64_t min, int64_t max, int precision) {
    int ret = hdr_init(min, max, precision, &hist);
    rt_assert(ret == 0);
  }

  ~HdrHistogram() { hdr_close(hist); }

  inline void record_value(size_t v) {
    hdr_record_value(hist, static_cast<int64_t>(v));
  }

  size_t percentile(double p) const {
    return static_cast<size_t>(hdr_value_at_percentile(hist, p));
  }

  void reset() { hdr_reset(hist); }

  hdr_histogram *get_raw_hist() { return hist; }

 private:
  hdr_histogram *hist = nullptr;
}
