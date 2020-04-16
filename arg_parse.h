#pragma once

#include <boost/program_options.hpp>
#include <string>

namespace arg_parse {

enum class OperationType {
  MAP,
  REDUCE
};

struct OperationSpec {
  OperationType type;
  std::string script_path;
  std::string src_file;
  std::string dst_file;
  uint32_t count;
};

bool Parse(int argc, char** argv, OperationSpec* spec);

} // namespace arg_parse
