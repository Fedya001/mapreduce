#!/usr/bin/env python3

import sys


# Cleans all labels and deletes invalid urls.
def main():
  for line in sys.stdin:
    url, _ = line.rstrip().split('\t')
    # Mininal url validation.
    if url.startswith("http"):
      print(url)


if __name__ == "__main__":
  main()

