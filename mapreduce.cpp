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

int MasterManager::Status::ExitCode() const {
  return (succeed_jobs_count < total_jobs_count) ? 1 : 0;
}

MasterManager::Status MasterManager::RunMappers(uint64_t count) const {
  uint64_t records_count = CountRecords(src_file_);

  count = std::min(static_cast<size_t>(count), records_count);
  size_t records_per_map = records_count / count;
  size_t extra_records_number = records_count % count;

  std::vector<size_t> jobs_sizes(count, records_per_map);
  std::for_each(jobs_sizes.begin(), jobs_sizes.begin() + extra_records_number,
                [](size_t& size) { ++size; });

  auto inputs = SplitRecordsIntoFiles(src_file_, jobs_sizes);
  Status status;
  auto outputs = Run(inputs, &status);
  JoinFiles(outputs, dst_file_);
  return status;
}

MasterManager::Status MasterManager::RunReducers() const {
  auto sorted_pile = Sort();
  auto inputs = SplitRecordsIntoFiles(std::move(sorted_pile));

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

  explicit Record(std::string line) {
    size_t tab_pos = line.find('\t');
    value = line.substr(tab_pos + 1);
    key = std::move(line);
    key.erase(tab_pos);
  }

  void DumpToFile(TmpFile& file) const {
    DumpToFile(file.Stream());
  }

  void DumpToFile(std::ostream& out) const {
    out << key << '\t' << value << '\n';
  }
};

std::vector<TmpFile> MasterManager::Run(std::vector<TmpFile>& inputs,
                                        Status* status) const {
  std::vector<TmpFile> outputs;
  outputs.reserve(inputs.size());

  std::vector<bp::child> children;
  children.reserve(inputs.size());

  auto prev_iter = inputs.begin();
  while (prev_iter != inputs.end()) {
    auto next_iter = prev_iter + std::min(
        kChildNumberLimit_,
        static_cast<uint64_t>(std::distance(prev_iter, inputs.end())));
    RunBatch(prev_iter, next_iter, outputs, status);
    prev_iter = next_iter;
  }

  return outputs;
}

template<class ForwardIt>
void MasterManager::RunBatch(
    ForwardIt first, ForwardIt last, std::vector<TmpFile>& outputs,
    MasterManager::Status* status) const {
  uint64_t batch_size = std::distance(first, last);
  std::vector<bp::child> children;
  children.reserve(batch_size);

  for (auto iter = first; iter != last; ++iter) {
    outputs.emplace_back(std::ios::in | std::ios::out);
    children.emplace_back(
        bp::search_path(script_path_,
                        {boost::filesystem::current_path()}).string(),
        bp::std_in < iter->GetPath().string(),
        bp::std_out > outputs.back().GetPath().string());
  }

  status->total_jobs_count += batch_size;
  for (auto&& child : children) {
    child.wait();
    if (child.exit_code() == 0) {
      ++status->succeed_jobs_count;
    }
  }
}

MasterManager::SortedPile MasterManager::Sort(uint64_t records_per_file) const {
  std::queue<SortedPile> piles;

  std::ifstream input(src_file_);
  while (input.peek() != EOF) {
    Records records_batch;
    records_batch.reserve(records_per_file);
    for (uint64_t id = 0; id < records_per_file && input.peek() != EOF; ++id) {
      std::string line;
      std::getline(input, line);
      records_batch.emplace_back(std::move(line));
    }
    std::sort(records_batch.begin(), records_batch.end());

    TmpFile batch(std::ios::out);
    for (const auto& record : records_batch) {
      record.DumpToFile(batch);
    }
    batch.Flush();

    SortedPile pile;
    pile.push(std::move(batch));
    piles.push(std::move(pile));
  }

  while (piles.size() > 1) {
    auto lhs = std::move(piles.front());
    piles.pop();
    auto rhs = std::move(piles.front());
    piles.pop();
    piles.push(MergePiles(std::move(lhs), std::move(rhs), records_per_file));
  }

  return std::move(piles.front());
}

MasterManager::SortedPile MasterManager::MergePiles(
    SortedPile left_pile, SortedPile right_pile, uint64_t records_per_file) {
  if (left_pile.empty()) { return right_pile; }
  if (right_pile.empty()) { return left_pile; }

  SortedPile result;

  uint64_t left_index = 0, right_index = 0;
  auto left_records = ExtractRecords(left_pile.front().GetPath().string());
  auto right_records = ExtractRecords(right_pile.front().GetPath().string());

  uint64_t records_printed = 0;
  TmpFile current_output(std::ios::in | std::ios::out);
  while (!left_pile.empty() || !right_pile.empty()) {
    if (!left_pile.empty() && (right_pile.empty() ||
        left_records[left_index] < right_records[right_index])) {
      // Read from left pile.
      left_records[left_index++].DumpToFile(current_output);
      if (left_index == left_records.size()) {
        left_pile.pop();
        if (!left_pile.empty()) {
          // Read next file from left pile.
          left_records = ExtractRecords(left_pile.front().GetPath().string());
          left_index = 0;
        }
      }
    } else {
      // Read from right pile.
      right_records[right_index++].DumpToFile(current_output);
      if (right_index == right_records.size()) {
        right_pile.pop();
        if (!right_pile.empty()) {
          // Read next file from right pile.
          right_records = ExtractRecords(right_pile.front().GetPath().string());
          right_index = 0;
        }
      }
    }

    ++records_printed;
    if (records_printed == records_per_file) {
      current_output.Flush();
      result.push(std::move(current_output));
      current_output = TmpFile(std::ios::in | std::ios::out);
      records_printed = 0;
    }
  }

  if (records_printed != 0) {
    current_output.Flush();
    result.push(std::move(current_output));
  }

  return result;
}

uint64_t MasterManager::CountRecords(const std::string& file) {
  std::ifstream src(file);
  return std::count(std::istreambuf_iterator<char>(src),
                    std::istreambuf_iterator<char>(), '\n');
}

MasterManager::Records MasterManager::ExtractRecords(const std::string& file) {
  Records records;

  std::ifstream src(file);
  while (src.peek() != EOF) {
    std::string buffer;
    std::getline(src, buffer);
    records.emplace_back(std::move(buffer));
  }

  return records;
}

std::vector<TmpFile> MasterManager::SplitRecordsIntoFiles(
    const std::string& file,
    const std::vector<size_t>& jobs_sizes) {
  std::vector<TmpFile> files;
  files.reserve(jobs_sizes.size());
  std::generate_n(std::back_inserter(files), jobs_sizes.size(),
                  [] { return TmpFile(std::ios::in | std::ios::out); });

  std::ifstream src(file);
  std::string line;
  for (size_t job_id = 0; job_id < jobs_sizes.size(); ++job_id) {
    for (size_t i = 0; i < jobs_sizes[job_id]; ++i) {
      std::getline(src, line);
      files[job_id].Stream() << line << '\n';
    }
    files[job_id].Stream().flush();
  }

  return files;
}

std::vector<TmpFile> MasterManager::SplitRecordsIntoFiles(
    MasterManager::SortedPile pile) {
  std::vector<TmpFile> files;

  std::optional<std::string> last_key;
  while (!pile.empty()) {
    auto tmp_file = std::move(pile.front());
    pile.pop();

    auto records = ExtractRecords(tmp_file.GetPath().string());
    for (const auto& record : records) {
      if (!last_key || *last_key != record.key) {
        files.emplace_back(std::ios::in | std::ios::out);
      }
      last_key = record.key;
      record.DumpToFile(files.back());
    }
  }

  for (auto&& file : files) {
    file.Flush();
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
