#include "arg_parse.h"

#include <iostream>
#include <stdexcept>

namespace arg_parse {

namespace po = boost::program_options;

bool Parse(int argc, char** argv, OperationSpec* spec) {
  try {
    po::options_description desc(
        "Usage mapreduce [MODE] [OPTIONS].\n\n"
        "Runs specified script either as a mapper or as a reducer.\n"
        "There are 2 modes for specifying it: map and reduce.\n"
        "Please, note that only one mode must be specified at a time.\n"
        "Both scripts work with tsv key-value files.\n\n"
        "Arguments");

    bool is_map, is_reduce;
    desc.add_options()
        ("help,h", "display this help")
        ("map", po::bool_switch(&is_map), "setup map mode")
        ("reduce", po::bool_switch(&is_reduce), "setup reduce mode")
        ("script_path,s", po::value<std::string>(), "path to map/reduce script")
        ("src_file,i", po::value<std::string>(), "tsv input file")
        ("dst_file,o", po::value<std::string>(), "tsv output file");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cout << desc << "\n";
      exit(0);
    }
    po::notify(vm);

    if (is_map && is_reduce) {
      std::cerr << "Only one mode must be specified at a time.\n";
      return false;
    }

    if (!is_map && !is_reduce) {
      std::cerr << "Mode must be specified.\n";
      return false;
    }

    auto script_path = vm.find("script_path");
    auto src_file = vm.find("src_file");
    auto dst_file = vm.find("dst_file");

    if (script_path == vm.end() || src_file == vm.end()
        || dst_file == vm.end()) {
      std::cerr << "Some options are missing.\n";
      return false;
    }

    spec->type = (is_map ? OperationType::MAP : OperationType::REDUCE);
    spec->script_path = script_path->second.as<std::string>();
    spec->src_file = src_file->second.as<std::string>();
    spec->dst_file = dst_file->second.as<std::string>();
  } catch (std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n";
    return false;
  }

  return true;
}

} // namespace arg_parse
