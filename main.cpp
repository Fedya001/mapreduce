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
        manager.RunMappers(spec.count);
        break;
      }
      case arg_parse::OperationType::REDUCE: {
        manager.RunReducers();
        break;
      };
      default: throw std::logic_error("Unsupported operation type");
    }
  } else {
    std::cerr << "Try 'mapreduce --help' for more information.\n";
    return 1;
  }

  return 0;
}
