#ifndef DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
#define DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <execution>
#include <fstream>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

#include "datafilter/datafilter_structs.hpp"
#include "iomanager/IOManager.hpp"

using namespace dunedaq::iomanager;

namespace dunedaq {
namespace datafilter {

struct RunInfo {
    std::atomic<unsigned int> run_number{0};
    std::atomic<unsigned int> file_index{0};
    mutable std::mutex mutex;  // mutable allows const methods to lock

    // Set the run information
    void set(unsigned int run, unsigned int file_idx) {
        std::lock_guard<std::mutex> lock(mutex);
        run_number = run;
        file_index = file_idx;
    }

    // Get the run information
    std::pair<unsigned int, unsigned int> get() const {
        std::lock_guard<std::mutex> lock(mutex);
        return {run_number.load(), file_index.load()};
    }
};

struct BookkeepingReceiver {
    RunInfo& run_info;

    // Thread control
    std::atomic<bool> stop_flag{false};
    std::unique_ptr<std::thread> receiver_thread;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    std::queue<nlohmann::json> bk_queue;
    std::atomic<unsigned int> received_cnt{0};

    explicit BookkeepingReceiver(RunInfo& info) : run_info(info) {
        TLOG() << "BookkeepingReceiver initialized";
    }

    ~BookkeepingReceiver() {
        stop();
        TLOG() << "BookkeepingReceiver destroyed";
    }

    void start() {
        std::lock_guard<std::mutex> lock(queue_mutex);
        if (receiver_thread) {
            TLOG() << "Receiver already running";
            return;
        }

        stop_flag.store(false);
        receiver_thread = std::make_unique<std::thread>([this]() {
            TLOG() << "Starting receiver thread (ID: "
                   << std::this_thread::get_id() << ")";
            this->receive_bk();
            TLOG() << "Receiver thread exiting";
        });
        TLOG() << "Bookkeeping receiver started";
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (!receiver_thread) {
                TLOG() << "No active receiver to stop";
                return;
            }

            TLOG() << "Initiating receiver shutdown";
            stop_flag.store(true);
            queue_cv.notify_all();
        }

        if (receiver_thread->joinable()) {
            receiver_thread->join();
        }
        receiver_thread.reset();
        TLOG() << "Bookkeeping receiver fully stopped";
    }

   private:
    void send_bk(dunedaq::datafilter::BookKeeping bk_info) {
        TLOG() << "Send bk to "
                  "FilterResultWriter=========================================="
                  "=====>";
        auto bookkeeping_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
                "bookkeeping1");
        if (!bookkeeping_sender) {
            TLOG() << "Failed to get bookkeeping sender!";
            return;
        }

        try {
            bookkeeping_sender->send(std::move(bk_info), Sender::s_block);
            TLOG() << "Successfully sent BookKeeping data.";

        } catch (const std::exception& e) {
            TLOG() << "Send failed: " << e.what();
        }
    }

    void receive_bk() {
        TLOG() << "Setting up bookkeeping receiver";

        auto cb_receiver =
            dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
                "bookkeeping0");
        if (!cb_receiver) {
            TLOG() << "Failed to get bookkeeping receiver";
            return;
        }

        std::string bk_file;
        std::mutex file_mutex;

        TLOG() << "Starting file writer thread";
        std::thread file_writer([this, &bk_file]() {
            TLOG() << "File writer thread started (ID: "
                   << std::this_thread::get_id() << ")";
            this->write_to_file(bk_file, stop_flag);
            TLOG() << "File writer thread exiting";
        });

        auto str_receiver_cb = [&](dunedaq::datafilter::BookKeeping bk) {
            if (stop_flag) return;

            unsigned int cnt = ++received_cnt;

            TLOG() << "Processing bookkeeping # " << cnt << " from "
                   << bk.from_id << " (Run: " << bk.run_number << ")";

            if (cnt == 1) {
                std::string file_index = "0";
                if (auto it = std::find_if(
                        bk.file_attributes_info.begin(),
                        bk.file_attributes_info.end(),
                        [](const auto& p) { return p.first == "file_index"; });
                    it != bk.file_attributes_info.end()) {
                    file_index = it->second;
                }

                send_bk(bk);
                run_info.set(bk.run_number, std::stoi(file_index));
                TLOG() << "Set initial run info - Run: " << bk.run_number
                       << " File Index: " << file_index;
            }

            auto [run, file_idx] = run_info.get();
            {
                std::lock_guard<std::mutex> lock(file_mutex);
                bk_file = generate_bk_filename(run, file_idx);
                TLOG() << "Updated output file: " << bk_file;
            }

            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                bk_queue.push(to_json(bk));
                TLOG() << "Queued message (Queue size: " << bk_queue.size()
                       << ")";
            }
            queue_cv.notify_one();
        };

        cb_receiver->add_callback(str_receiver_cb);
        TLOG() << "Callback registered, entering main loop";

        while (!stop_flag) {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (queue_cv.wait_for(
                    lock, std::chrono::milliseconds(100),
                    [this]() { return !bk_queue.empty() || stop_flag; })) {
                while (!bk_queue.empty()) {
                    auto msg = bk_queue.front();
                    bk_queue.pop();
                    lock.unlock();

                    // Process message
                    TLOG() << "Processing queued message: " << msg.dump();

                    lock.lock();
                }
            }
        }

        TLOG() << "Cleaning up receiver";
        cb_receiver->remove_callback();

        if (file_writer.joinable()) {
            TLOG() << "Waiting for file writer to finish";
            file_writer.join();
        }
        TLOG() << "Receiver cleanup complete";
    }

    nlohmann::json to_json(const BookKeeping& bk) {
        return nlohmann::json{{"entry_id", bk.entry_id},
                              {"conn_id", bk.conn_id},
                              {"from_id", bk.from_id},
                              {"data_filter_id", bk.data_filter_id},
                              {"node", bk.node},
                              {"tr_header_info", bk.tr_header_info},
                              {"file_attributes_info", bk.file_attributes_info},
                              {"tr_status", bk.tr_status},
                              {"file_send_list", bk.file_send_list},
                              {"file_send_status", bk.file_send_status},
                              {"transfer_rate", bk.transfer_rate}};
    }

    // Function to read existing transactions from the file
    nlohmann::json open_existing_bk(const std::string& filename) {
        std::ifstream file(filename);
        if (file.is_open()) {
            try {
                nlohmann::json existing_bk;
                file >> existing_bk;
                return existing_bk;
            } catch (const std::exception& e) {
                std::cerr << "Error reading JSON file: " << e.what()
                          << std::endl;
            }
        }
        return nlohmann::json::array();  // Return an empty array if the file
                                         // doesn't exist or is invalid
    }

    void write_to_file(const std::string& filename,
                       std::atomic<bool>& stop_flag) {
        nlohmann::json existing_bk = open_existing_bk(filename);

        auto start_time = std::chrono::high_resolution_clock::now();
        int transaction_count = 0;

        while (true) {
            std::unique_lock<std::mutex> lock(queue_mutex);

            // Use a lambda to wait for the condition variable
            queue_cv.wait(lock, [&] { return !bk_queue.empty(); });

            nlohmann::json transaction = bk_queue.front();
            bk_queue.pop();
            lock.unlock();

            if (existing_bk.is_array()) {
                existing_bk.push_back(transaction);
            } else {
                std::cerr << "Error: Existing transactions is not an array. "
                             "Cannot append."
                          << std::endl;
                continue;
            }

            std::ofstream file(filename);
            if (file.is_open()) {
                file << existing_bk.dump(4);
            } else {
                std::cerr << "Failed to open file for writing!" << std::endl;
            }
            transaction_count++;

            if (transaction_count % 1 == 0) {
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
                std::cout << "Processed " << transaction_count
                          << " transactions in " << duration << " ms"
                          << std::endl;
            }
        }
    }

    std::string generate_bk_filename(int run_number, int file_index) {
        std::ostringstream filename_oss;
        filename_oss << "bookkeeping_" << std::setw(6) << std::setfill('0')
                     << run_number << "_" << std::setw(4) << std::setfill('0')
                     << file_index << ".json";
        return filename_oss.str();
    }
};

}  // namespace datafilter
}  // namespace dunedaq

#endif  // DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
