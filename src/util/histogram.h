#pragma once
// HDR-style latency histogram for benchmark percentile tracking.

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kv {

// Simple histogram tracking latency in microseconds.
// Uses logarithmic bucketing for wide dynamic range.
class LatencyHistogram {
public:
    LatencyHistogram() { Reset(); }

    void Reset() {
        std::fill(buckets_.begin(), buckets_.end(), 0);
        count_ = 0;
        sum_ = 0;
        min_ = UINT64_MAX;
        max_ = 0;
    }

    void Record(uint64_t value_us) {
        count_++;
        sum_ += value_us;
        min_ = std::min(min_, value_us);
        max_ = std::max(max_, value_us);
        buckets_[BucketIndex(value_us)]++;
    }

    [[nodiscard]] uint64_t Count() const { return count_; }
    [[nodiscard]] uint64_t Min() const { return count_ ? min_ : 0; }
    [[nodiscard]] uint64_t Max() const { return max_; }
    [[nodiscard]] double Mean() const { return count_ ? static_cast<double>(sum_) / count_ : 0; }

    // Get percentile value (0-100).
    [[nodiscard]] uint64_t Percentile(double pct) const {
        if (count_ == 0) return 0;
        uint64_t target = static_cast<uint64_t>(std::ceil(pct / 100.0 * count_));
        uint64_t cumulative = 0;
        for (size_t i = 0; i < buckets_.size(); i++) {
            cumulative += buckets_[i];
            if (cumulative >= target) {
                return BucketUpperBound(i);
            }
        }
        return max_;
    }

    [[nodiscard]] uint64_t P50() const { return Percentile(50); }
    [[nodiscard]] uint64_t P95() const { return Percentile(95); }
    [[nodiscard]] uint64_t P99() const { return Percentile(99); }

    // Export as CSV line: count,min,max,mean,p50,p95,p99
    [[nodiscard]] std::string ToCSV() const {
        return std::to_string(count_) + "," +
               std::to_string(Min()) + "," +
               std::to_string(Max()) + "," +
               std::to_string(static_cast<uint64_t>(Mean())) + "," +
               std::to_string(P50()) + "," +
               std::to_string(P95()) + "," +
               std::to_string(P99());
    }

    static constexpr const char* CSVHeader() {
        return "count,min_us,max_us,mean_us,p50_us,p95_us,p99_us";
    }

private:
    // 128 buckets covering 0 to ~2^32 microseconds (~71 minutes).
    // Bucket 0: [0, 1), Bucket 1: [1, 2), Bucket 2: [2, 4), ...
    static constexpr size_t kNumBuckets = 128;

    static size_t BucketIndex(uint64_t value) {
        if (value == 0) return 0;
        // floor(log2(value)) + 1, capped
        size_t idx = 64 - __builtin_clzll(value);
        return std::min(idx, kNumBuckets - 1);
    }

    static uint64_t BucketUpperBound(size_t idx) {
        if (idx == 0) return 0;
        if (idx >= 63) return UINT64_MAX;
        return (1ULL << idx) - 1;
    }

    std::array<uint64_t, kNumBuckets> buckets_{};
    uint64_t count_ = 0;
    uint64_t sum_ = 0;
    uint64_t min_ = UINT64_MAX;
    uint64_t max_ = 0;
};

} // namespace kv
