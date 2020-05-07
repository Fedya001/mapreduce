#include "mapreduce.h"

#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <vector>

namespace mapreduce {

namespace bp = boost::process;

MasterManager::MasterManager(std::string script_path, std::string src_file,
                             std::string dst_file)
    : script_path_(std::move(script_path)),
      src_file_(std::move(src_file)),
      dst_file_(std::move(dst_file)) {}

int MasterManager::Status::ExitCode() const {
  return (succeed_jobs_count < total_jobs_count) ? 1 : 0;
}

MasterManager::Status MasterManager::RunMappers(uint64_t count) const {
  auto records = ExtractRecords(src_file_);

  count = std::min(static_cast<size_t>(count), records.size());
  size_t records_per_map = records.size() / count;
  size_t extra_records_number = records.size() % count;

  std::vector<size_t> jobs_sizes(count, records_per_map);
  std::for_each(jobs_sizes.begin(), jobs_sizes.begin() + extra_records_number,
                [](size_t& size) { ++size; });

  auto inputs = SplitRecordsIntoFiles(records, jobs_sizes);
  Status status;
  auto outputs = Run(inputs, &status);
  JoinFiles(outputs, dst_file_);
  return status;
}

MasterManager::Status MasterManager::RunReducers() const {
  auto records = ExtractRecords(src_file_);
  std::sort(records.begin(), records.end());

  std::vector<size_t> jobs_sizes;
  auto iter = records.begin();

  while (iter != records.end()) {
    auto next = std::adjacent_find(
        iter, records.end(), [](const auto& lhs, const auto& rhs) {
          return lhs.key != rhs.key;
        });
    jobs_sizes.push_back(std::distance(iter, next));
    iter = next;
    if (iter != records.end()) {
      ++iter;
      ++jobs_sizes.back();
    }
  }

  auto inputs = SplitRecordsIntoFiles(records, jobs_sizes);
  Status status;
  auto outputs = Run(inputs, &status);
  JoinFiles(outputs, dst_file_);
  return status;
}

const std::string& MasterManager::GetScriptPath() const {
  return script_path_;
}

const std::string& MasterManager::GetSrcFile() const {
  return src_file_;
}

const std::string& MasterManager::GetDstFile() const {
  return dst_file_;
}

void MasterManager::SetScriptPath(std::string script_path) {
  script_path_ = std::move(script_path);
}

void MasterManager::SetSrcFile(std::string src_file) {
  src_file_ = std::move(src_file);
}

void MasterManager::SetDstFile(std::string dst_file) {
  dst_file_ = std::move(dst_file);
}

struct MasterManager::Record {
  std::string key;
  std::string value;

  Record(std::string key, std::string value)
      : key(std::move(key)), value(std::move(value)) {}

  void DumpToFile(TmpFile& file) const {
    file.Stream() << key << '\t' << value << '\n';
  }
};

std::vector<TmpFile> MasterManager::Run(std::vector<TmpFile>& inputs,
                                        Status* status) const {
  std::vector<TmpFile> outputs;
  outputs.reserve(inputs.size());

  std::vector<bp::child> children;
  children.reserve(inputs.size());
  for (auto&& input : inputs) {
    outputs.emplace_back(std::ios::in | std::ios::out);
    children.emplace_back(
        bp::search_path(script_path_,
                        {boost::filesystem::current_path()}).string(),
        bp::std_in < input.GetPath().string(),
        bp::std_out > outputs.back().GetPath().string());
  }

  status->total_jobs_count = inputs.size();
  for (auto&& child : children) {
    child.wait();
    if (child.exit_code() == 0) {
      ++status->succeed_jobs_count;
    }
  }

  return outputs;
}

MasterManager::Records MasterManager::ExtractRecords(
    const std::string& file) {
  Records records;

  std::ifstream src(file);
  while (src.peek() != EOF) {
    std::string buffer;
    std::getline(src, buffer);
    size_t tab_pos = buffer.find('\t');
    auto value = buffer.substr(tab_pos + 1);
    buffer.erase(tab_pos);
    records.emplace_back(std::move(buffer), std::move(value));
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
      records[record_id++].DumpToFile(files[job_id]);
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

bool operator<(const MasterManager::Record& lhs,
               const MasterManager::Record& rhs) {
  return lhs.key < rhs.key;
}

std::ostream& operator<<(std::ostream& out, MasterManager::Status result) {
  return out << result.succeed_jobs_count << " out of "
             << result.total_jobs_count << " child processed succeed. "
             << "Exiting with code " << result.ExitCode() << '.';
}

} // namespace mapreduce
