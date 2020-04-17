#pragma once

#include "arg_parse.h"
#include "utils/tmpfile.h"

namespace mapreduce {

class MasterManager {
 public:
  typedef std::vector<std::string> Records;

  MasterManager(std::string script_path, std::string src_file,
                std::string dst_file);

  void RunMappers(uint32_t count) const;
  void RunReducers() const;

 private:
  void Sort() const;

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

} // namespace mapreduce
