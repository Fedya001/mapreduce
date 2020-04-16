#include "arg_parse.h"

#include <boost/process.hpp>
#include <iostream>

int main(int argc, char** argv) {
  arg_parse::OperationSpec spec;
  if (arg_parse::Parse(argc, argv, &spec)) {
    switch (spec.type) {
      case arg_parse::OperationType::MAP:
        std::cout << "map\n";
        break;
      case arg_parse::OperationType::REDUCE:
        std::cout << "reduce\n";
        break;
      default: throw std::logic_error("Unsupported operation type");
    }

    std::cout << "script = " << spec.script_path << '\n'
              << "src file = " << spec.src_file << '\n'
              << "dst file = " << spec.dst_file << '\n';
  } else {
    std::cerr << "Try 'mapreduce --help' for more information.\n";
    return 1;
  }

  return 0;
}
