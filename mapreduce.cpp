#include "mapreduce.h"

#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <vector>
#include <iostream>

namespace mapreduce {

namespace bp = boost::process;

MasterManager::MasterManager(std::string script_path, std::string src_file,
                             std::string dst_file)
    : script_path_(std::move(script_path)),
      src_file_(std::move(src_file)),
      dst_file_(std::move(dst_file)) {}

void MasterManager::RunMappers(uint32_t count) const {
  auto records = ExtractRecords(src_file_);

  count = std::min(static_cast<size_t>(count), records.size());
  size_t records_per_map = records.size() / count;
  size_t extra_records_number = records.size() % count;

  std::vector<size_t> jobs_sizes(count, records_per_map);
  std::for_each(jobs_sizes.begin(), jobs_sizes.begin() + extra_records_number,
                [](size_t& size) { ++size; });
  auto inputs = SplitRecordsIntoFiles(records, jobs_sizes);

  std::vector<TmpFile> outputs;
  outputs.reserve(count);
  std::generate_n(std::back_inserter(outputs), count,
                  [] { return TmpFile(std::ios::in | std::ios::out); });

  std::vector<bp::child> children;
  for (size_t map_id = 0; map_id < count; ++map_id) {
    children.emplace_back(
        bp::search_path(script_path_,
                        {boost::filesystem::current_path()}).string(),
        bp::std_in < inputs[map_id].GetPath().string(),
        bp::std_out > outputs[map_id].GetPath().string());
  }

  for (auto&& child : children) {
    child.wait();
  }

  JoinFiles(outputs, dst_file_);
}

void MasterManager::RunReducers() const {
  Sort();
  // TODO : implement it
}

void MasterManager::Sort() const {
  // TODO : implement it
}

MasterManager::Records MasterManager::ExtractRecords(
    const std::string& file) {
  Records records;

  std::ifstream src(file);
  while (src.peek() != EOF) {
    records.emplace_back();
    std::getline(src, records.back());
  }

  return records;
}

std::vector<TmpFile> MasterManager::SplitRecordsIntoFiles(
    const MasterManager::Records& records,
    const std::vector<size_t>& jobs_sizes) {
  std::vector<TmpFile> files;
  files.reserve(jobs_sizes.size());
  std::generate_n(std::back_inserter(files), jobs_sizes.size(),
                  [] { return TmpFile(std::ios::in | std::ios::out); });

  size_t record_id = 0;
  for (size_t job_id = 0; job_id < jobs_sizes.size(); ++job_id) {
    for (size_t i = 0; i < jobs_sizes[job_id]; ++i) {
      files[job_id].Stream() << records[record_id++] << '\n';
    }
    files[job_id].Stream().flush();
  }

  return files;
}

void MasterManager::JoinFiles(std::vector<TmpFile>& files,
                              const std::string& joined_file) {
  std::string buffer;
  std::ofstream joined(joined_file);
  for (auto&& file : files) {
    while (file.Stream().peek() != EOF) {
      std::getline(file.Stream(), buffer);
      joined << buffer << '\n';
    }
  }
}

} // namespace mapreduce
