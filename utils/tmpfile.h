#pragma once

#include <boost/filesystem.hpp>
#include <fstream>
#include <string>

class TmpFile {
 public:
  explicit TmpFile(std::ios::openmode mod);
  ~TmpFile();

  TmpFile(const TmpFile&) = delete;
  TmpFile(TmpFile&&) = default;

  TmpFile& operator=(const TmpFile&) = delete;
  TmpFile& operator=(TmpFile&&) = default;

  void Reopen(std::ios::openmode mod);
  std::fstream& Stream();

  boost::filesystem::path GetPath() const;

 private:
  boost::filesystem::path path_;
  std::fstream stream_;
};
