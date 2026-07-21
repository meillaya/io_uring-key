#pragma once
// Thread-pool I/O backend: std::thread pool dispatching the same ops as sync.

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "io/io_backend.h"
#include "io/sync_backend.h"

namespace kv {

class ThreadPoolBackend final : public IOBackend {
public:
    explicit ThreadPoolBackend(size_t num_threads = 4)
        : sync_(std::make_unique<SyncBackend>()), stop_(false) {
        for (size_t i = 0; i < num_threads; i++) {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
    }

    ~ThreadPoolBackend() override {
        {
            std::lock_guard lock(queue_mu_);
            stop_ = true;
        }
        queue_cv_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    [[nodiscard]] std::string_view Name() const override { return "threadpool"; }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        return sync_->Open(path, create, direct, out);
    }

    Status Close(FileHandle fh) override { return sync_->Close(fh); }
    Status Delete(std::string_view path) override { return sync_->Delete(path); }
    Status FileSize(FileHandle fh, uint64_t* size) override {
        return sync_->FileSize(fh, size);
    }

    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        return Dispatch([this, fh, offset, buf, len] {
            return sync_->Read(fh, offset, buf, len);
        });
    }

    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        return Dispatch([this, fh, offset, buf, len] {
            return sync_->Write(fh, offset, buf, len);
        });
    }

    Status Append(FileHandle fh, const void* buf, size_t len,
                  uint64_t* offset) override {
        return Dispatch([this, fh, buf, len, offset] {
            return sync_->Append(fh, buf, len, offset);
        });
    }

    Status Sync(FileHandle fh, bool data_only) override {
        return Dispatch([this, fh, data_only] {
            return sync_->Sync(fh, data_only);
        });
    }

    Status SubmitBatch(std::span<const IORequest> reqs,
                       std::span<IOResult> results) override {
        if (reqs.size() != results.size())
            return Status::InvalidArgument("reqs/results size mismatch");

        std::vector<std::future<IOResult>> futures;
        futures.reserve(reqs.size());
        for (size_t i = 0; i < reqs.size(); i++) {
            auto req = reqs[i];
            futures.push_back(DispatchAsync([this, req] {
                return sync_->ExecuteSync(req);
            }));
        }

        for (size_t i = 0; i < futures.size(); i++) {
            results[i] = futures[i].get();
            if (!results[i].status.ok()) return results[i].status;
        }
        return Status::OK();
    }

    [[nodiscard]] bool SupportsDirectIO() const override { return true; }
    [[nodiscard]] bool SupportsBatchedIO() const override { return true; }

private:
    using Task = std::function<Status()>;

    Status Dispatch(Task task) {
        auto future = DispatchAsync(std::move(task));
        return future.get();
    }

    template <typename F>
    auto DispatchAsync(F&& f) -> std::future<decltype(f())> {
        using R = decltype(f());
        auto promise = std::make_shared<std::promise<R>>();
        auto future = promise->get_future();
        {
            std::lock_guard lock(queue_mu_);
            queue_.push([promise, f = std::forward<F>(f)]() mutable {
                try {
                    promise->set_value(f());
                } catch (...) {
                    promise->set_exception(std::current_exception());
                }
            });
        }
        queue_cv_.notify_one();
        return future;
    }

    void WorkerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(queue_mu_);
                queue_cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                task = std::move(queue_.front());
                queue_.pop();
            }
            task();
        }
    }

    std::unique_ptr<SyncBackend> sync_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    bool stop_;
};

} // namespace kv
