#include <iostream>
#include "tmpfile.h"

TmpFile::TmpFile(std::ios::openmode mode)
    : path_(boost::filesystem::unique_path()),
      stream_(path_.string(), mode | std::ios::trunc) {}

TmpFile::~TmpFile() {
  boost::filesystem::remove(path_);
}

void TmpFile::Reopen(std::ios::openmode mode) {
  stream_.close();
  stream_.open(path_.string(), mode);
}

std::fstream& TmpFile::Stream() {
  return stream_;
}
