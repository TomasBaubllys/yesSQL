#ifndef YSQL_THREAD_POOL_H_INCLUDED
#define YSQL_THREAD_POOL_H_INCLUDED

#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <stdexcept>

#define THREAD_POOL_ENQUEUE_STOPPED_ERR_MSG "Thread pool enqueue stopped working\n"

class Thread_Pool {
public:
    explicit Thread_Pool(size_t num_threads = std::thread::hardware_concurrency())
        : stop(false)
    {
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back(std::thread([this]() {
                while(true) { 
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        condition.wait(lock, [this]() {
                            return stop || !tasks.empty();
                        });

                        if (stop && tasks.empty()) {
                            return;
						}

                        task = tasks.front();
                        tasks.pop();
                    } 

                    task();
                }
            }));
        }
    }

    template <typename F, typename... Args>
    std::future<typename std::invoke_result<F, Args...>::type>
    enqueue(F&& f, Args&&... args)
    {
        typedef typename std::invoke_result<F, Args...>::type return_type;

        std::shared_ptr<std::packaged_task<return_type()>> task_ptr (
            new std::packaged_task<return_type()>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
            )
        );

        std::future<return_type> result_future = task_ptr -> get_future();

        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop)
                throw std::runtime_error(THREAD_POOL_ENQUEUE_STOPPED_ERR_MSG);

            tasks.emplace([task_ptr]() { (*task_ptr)(); });
        }

        condition.notify_one();

        return result_future;
    }

    ~Thread_Pool()
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }

        condition.notify_all();

		for(std::thread& worker : workers) {
			worker.join();
		}
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

#endif // YSQL_THREAD_POOL_H_INCLUDED
