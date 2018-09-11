Latency to flush a write from remote NIC cache

DRAM writes:
 * 16-byte inline WRITE latency = 1.3 us (median and 99th percentile)
 * 16-byte inline WRITE latency with a READ flush, without DDIO = 2.6 us 50%, 2.9 us 99%
 * 16-byte inline WRITE latency with a READ flush, without DDIO = 2.4 us 50%, 2.6 us 99%
