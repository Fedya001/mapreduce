#include "arg_parse.h"
#include "mapreduce.h"

#include <iostream>

int main(int argc, char** argv) {
  arg_parse::OperationSpec spec;
  if (arg_parse::Parse(argc, argv, &spec)) {
    auto manager = mapreduce::MasterManager(
        spec.script_path, spec.src_file, spec.dst_file);

    switch (spec.type) {
      case arg_parse::OperationType::MAP: {
        auto status = manager.RunMappers(spec.count);
        std::cout << status << '\n';
        return status.ExitCode();
      }
      case arg_parse::OperationType::REDUCE: {
        auto status = manager.RunReducers();
        std::cout << status << '\n';
        return status.ExitCode();
      };
      default: throw std::logic_error("Unsupported operation type");
    }
  } else {
    std::cerr << "Try 'mapreduce --help' for more information.\n";
    return 1;
  }

  return 0;
}
