#include "mapreduce.h"

#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <fstream>
#include <vector>
#include <iostream>

namespace mapreduce {

namespace bp = boost::process;

MasterManager::MasterManager(std::string script_path, std::string src_file,
                             std::string dst_file)
    : script_path_(std::move(script_path)),
      src_file_(std::move(src_file)),
      dst_file_(std::move(dst_file)) {}

void MasterManager::RunMappers(uint32_t /* count */) const {
  // TODO : implement it
}

void MasterManager::RunReducers() const {
  Sort();
  // TODO : implement it
}

void MasterManager::Sort() const {
  // TODO : implement it
}

} // namespace mapreduce
