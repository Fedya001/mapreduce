#pragma once

#include "arg_parse.h"
#include "utils/tmpfile.h"

namespace mapreduce {

class MasterManager {
 public:
  MasterManager(std::string script_path, std::string src_file,
                std::string dst_file);

  struct Status {
    int64_t succeed_jobs_count = 0;
    int64_t total_jobs_count = 0;

    [[nodiscard]] int ExitCode() const;
  };

  [[nodiscard]] Status RunMappers(uint64_t count) const;
  [[nodiscard]] Status RunReducers() const;

 private:
  struct Record;
  typedef std::vector<Record> Records;

  friend bool operator<(const Record&, const Record&);

  std::vector<TmpFile> Run(std::vector<TmpFile>& inputs, Status* status) const;

  static Records ExtractRecords(const std::string& file);
  static std::vector<TmpFile> SplitRecordsIntoFiles(
      const Records& records, const std::vector<size_t>& jobs_sizes);
  static void JoinFiles(std::vector<TmpFile>& files,
                        const std::string& joined_file);

 private:
  std::string script_path_;
  std::string src_file_;
  std::string dst_file_;
};

std::ostream& operator<<(std::ostream& out, MasterManager::Status result);

} // namespace mapreduce
