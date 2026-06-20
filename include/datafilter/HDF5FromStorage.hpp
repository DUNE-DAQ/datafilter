#ifndef DFBACKEND_INCLUDE_HDF5FromStorage_HPP_
#define DFBACKEND_INCLUDE_HDF5FromStorage_HPP_

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "logging/Logging.hpp"
#include "nlohmann/json.hpp"

namespace dunedaq {
namespace datafilter {

struct HDF5FromStorage {
    nlohmann::json hdf5_files_json;
    std::vector<std::string> hdf5_files_already_transfer;
    std::vector<std::filesystem::path> hdf5_files_to_transfer;
    std::vector<std::filesystem::path> hdf5_files_waiting;
    std::vector<std::filesystem::path> hdf5_filtered_files;
    std::vector<std::filesystem::path> hdf5_filtered_writing;
    const std::string json_file;
    const std::string storage_pathname;
    bool is_save_json = false;

    HDF5FromStorage(const std::string& storage_pathname,
                    const std::string json_file)
        : storage_pathname(storage_pathname), json_file(json_file) {
        ReadJSON();  // Read the JSON file
        HDF5Find();  // Scan the storage directory
    }

    void HDF5Find() {
        const std::filesystem::path daq_storage_path{storage_pathname};
        const auto now = std::filesystem::file_time_type::clock::now();
        const auto one_hour_ago = now - std::chrono::hours(1);

        // If the storage path does not exist (e.g. filesystem not yet mounted),
        // skip the scan and leave all lists empty.  WriteJSON can still run.
        if (!std::filesystem::exists(daq_storage_path)) {
            TLOG() << "HDF5FromStorage: storage path does not exist, skipping"
                      " scan: " << storage_pathname;
            return;
        }

        for (auto const& entry :
             std::filesystem::directory_iterator{daq_storage_path}) {
            std::string filename = entry.path().filename().string();

            // Classify by file state markers:
            //   *.filtered.hdf5    — filter complete
            //   *.filtered.writing — filter in progress
            //   *.writing          — DAQ still writing (raw)
            //   *.hdf5             — raw, ready for filtering
            if (filename.size() > 14 &&
                filename.substr(filename.size() - 14) == ".filtered.hdf5") {
                hdf5_filtered_files.push_back(entry.path());
            } else if (filename.size() > 17 &&
                       filename.substr(filename.size() - 17) ==
                           ".filtered.writing") {
                if (std::filesystem::file_size(entry) == 0) {
                    TLOG() << "HDF5FromStorage: removing empty partial file: "
                           << filename;
                    std::filesystem::remove(entry.path());
                } else {
                    hdf5_filtered_writing.push_back(entry.path());
                }
            } else if (filename.size() > 8 &&
                       filename.substr(filename.size() - 8) == ".writing") {
                hdf5_files_waiting.push_back(entry.path());
            } else if (filename.size() > 5 &&
                       filename.substr(filename.size() - 5) == ".hdf5") {
                auto mod_time = std::filesystem::last_write_time(entry);
                bool is_older_than_one_hour = (mod_time < one_hour_ago);

                if (is_older_than_one_hour) {
                    TLOG_DEBUG(7)
                        << "found a new hdf5 file older than one hour: "
                        << entry.path().parent_path().string() << "/"
                        << filename << '\n';

                    bool is_already_transferred = false;
                    for (const auto& item : hdf5_files_already_transfer) {
                        if (item == filename) {
                            is_already_transferred = true;
                            break;
                        }
                    }

                    if (!is_already_transferred) {
                        TLOG() << "To transfer " << filename << '\n';
                        hdf5_files_to_transfer.push_back(entry.path());
                    }
                }
            }
        }

        // Remove duplicates (if any)
        if (!hdf5_files_to_transfer.empty()) {
            std::sort(hdf5_files_to_transfer.begin(),
                      hdf5_files_to_transfer.end());
            hdf5_files_to_transfer.erase(
                std::unique(hdf5_files_to_transfer.begin(),
                            hdf5_files_to_transfer.end()),
                hdf5_files_to_transfer.end());
        }
    }

    void ReadJSON() {
        // If the JSON file does not exist or is empty, start with an empty
        // transfer list.  This is safe on a fresh or remounted filesystem --
        // WriteJSON will create the file on the first successful write.
        if (!std::filesystem::exists(json_file)) {
            TLOG() << "HDF5FromStorage: JSON file not found, starting with"
                      " empty transfer list: " << json_file;
            return;
        }

        std::ifstream file_in(json_file);
        if (!file_in.is_open() ||
            file_in.peek() == std::ifstream::traits_type::eof()) {
            TLOG() << "HDF5FromStorage: JSON file empty or unreadable,"
                      " starting fresh: " << json_file;
            return;
        }

        try {
            // Parse the JSON file
            file_in >> hdf5_files_json;
            file_in.close();

            // Check if the "hdf5_files" key exists
            if (!hdf5_files_json.contains("hdf5_files")) {
                TLOG_DEBUG(7)
                    << "Key 'hdf5_files' not found in JSON file." << '\n';
                return;
            }

            // Validate that "hdf5_files" is an array
            if (!hdf5_files_json["hdf5_files"].is_array()) {
                TLOG_DEBUG(7)
                    << "Expected 'hdf5_files' to be an array." << '\n';
                return;
            }

            // Iterate through the array
            for (const auto& hdf5_file : hdf5_files_json["hdf5_files"]) {
                // Check if the "hdf5_file" key exists and is a string
                if (!hdf5_file.contains("hdf5_file")) {
                    TLOG_DEBUG(7)
                        << "Key 'hdf5_file' not found in JSON array entry."
                        << '\n';
                    continue;  // Skip this entry
                }

                if (!hdf5_file["hdf5_file"].is_string()) {
                    TLOG_DEBUG(7)
                        << "Expected 'hdf5_file' to be a string." << '\n';
                    continue;  // Skip this entry
                }

                // Add the file name (as a string) to the already_transfer list
                hdf5_files_already_transfer.push_back(
                    hdf5_file["hdf5_file"].get<std::string>());
            }

            // Remove duplicates
            std::sort(hdf5_files_already_transfer.begin(),
                      hdf5_files_already_transfer.end());
            hdf5_files_already_transfer.erase(
                std::unique(hdf5_files_already_transfer.begin(),
                            hdf5_files_already_transfer.end()),
                hdf5_files_already_transfer.end());
        } catch (const nlohmann::json::exception& e) {
            TLOG() << "HDF5FromStorage: JSON parse error, starting fresh: " << e.what();
            hdf5_files_already_transfer.clear();
        }
    }
    void WriteJSON(const std::string& filepath) {
        try {
            // Extract the filename from the complete path
            std::filesystem::path path_obj(filepath);
            std::string filename =
                path_obj.filename().string();  // e.g., "file1.hdf5"

            // Read and parse the existing JSON data.  If the file is absent
            // or empty (e.g. fresh mount), start from an empty array so
            // WriteJSON can still record the file and unblock the retry loop.
            nlohmann::json json_data;
            std::ifstream file_in(json_file);
            if (!file_in.is_open() ||
                file_in.peek() == std::ifstream::traits_type::eof()) {
                json_data["hdf5_files"] = nlohmann::json::array();
            } else {
                try {
                    file_in >> json_data;
                } catch (const std::exception& e) {
                    TLOG() << "HDF5FromStorage: WriteJSON JSON parse error, resetting: "
                           << e.what();
                    json_data["hdf5_files"] = nlohmann::json::array();
                }
                file_in.close();
            }

            // Check if the "hdf5_files" key exists
            if (!json_data.contains("hdf5_files")) {
                // If the key doesn't exist, create it as an empty array
                json_data["hdf5_files"] = nlohmann::json::array();
            }

            // Validate that "hdf5_files" is an array
            if (!json_data["hdf5_files"].is_array()) {
                TLOG() << "HDF5FromStorage: 'hdf5_files' is not an array, resetting.";
                json_data["hdf5_files"] = nlohmann::json::array();
            }

            // Check if the filename already exists in the JSON data
            bool is_duplicate = false;
            for (const auto& entry : json_data["hdf5_files"]) {
                if (entry.contains("hdf5_file") &&
                    entry["hdf5_file"] == filename) {
                    is_duplicate = true;
                    break;
                }
            }

            // If the filename is not a duplicate, add it to the JSON data
            if (!is_duplicate) {
                // Create a new entry for the filename
                nlohmann::json new_entry;
                new_entry["hdf5_file"] = filename;

                // Add the new entry to the "hdf5_files" array
                json_data["hdf5_files"].push_back(new_entry);

                // Open the JSON file for writing
                std::ofstream file_out(json_file);
                if (!file_out.is_open()) {
                    throw std::runtime_error(
                        "Failed to open JSON file for writing: " + json_file);
                }

                // Write the updated JSON data to the file
                file_out << std::setw(4) << json_data << std::endl;
                file_out.close();

                // Update the in-memory JSON object
                hdf5_files_json = json_data;

                // Add the filename to the already_transfer list (if not a
                // duplicate)
                if (std::find(hdf5_files_already_transfer.begin(),
                              hdf5_files_already_transfer.end(),
                              filename) == hdf5_files_already_transfer.end()) {
                    hdf5_files_already_transfer.push_back(filename);

                    // Remove duplicates (if any)
                    std::sort(hdf5_files_already_transfer.begin(),
                              hdf5_files_already_transfer.end());
                    hdf5_files_already_transfer.erase(
                        std::unique(hdf5_files_already_transfer.begin(),
                                    hdf5_files_already_transfer.end()),
                        hdf5_files_already_transfer.end());
                }
            } else {
                TLOG_DEBUG(7)
                    << "Filename '" << filename
                    << "' is already in the JSON file. Skipping duplicate."
                    << '\n';
            }
        } catch (const std::exception& e) {
            TLOG() << "HDF5FromStorage: WriteJSON error (not re-thrown): " << e.what();
        }
    }

    void save(const std::string json_file) {
        std::ofstream file_out(json_file);
        nlohmann::json j, new_entry;
        j = hdf5_files_json;

        file_out << std::setw(4) << j << std::endl;
        file_out.close();
    }

    void print() {
        TLOG_DEBUG(7) << "print hdf5 files info"
                      << "\n";
        for (auto file : hdf5_files_to_transfer) {
            std::cout << "HDF5 file to transfer " << file << "\n";
        }
        for (auto file : hdf5_files_waiting) {
            std::cout << "HDF5 file waiting (DAQ writing) " << file << "\n";
        }
        for (auto file : hdf5_filtered_files) {
            std::cout << "HDF5 filtered complete " << file << "\n";
        }
        for (auto file : hdf5_filtered_writing) {
            std::cout << "HDF5 filtered in-progress " << file << "\n";
        }
        for (auto file : hdf5_files_already_transfer) {
            std::cout << "HDF5 file already transfer " << file << "\n";
        }
    }
};
}  // namespace datafilter
}  // namespace dunedaq
#endif
