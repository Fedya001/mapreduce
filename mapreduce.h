#pragma once

#include "arg_parse.h"
#include "utils/tmpfile.h"

namespace mapreduce {

class MasterManager {
 public:
  MasterManager(std::string script_path, std::string src_file,
                std::string dst_file);

  void RunMappers(uint32_t count) const;
  void RunReducers() const;

 private:
  void Sort() const;

 private:
  std::string script_path_;
  std::string src_file_;
  std::string dst_file_;
};

} // namespace mapreduce
