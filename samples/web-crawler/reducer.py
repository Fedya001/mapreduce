#!/usr/bin/env python3

import sys


def is_visited(value):
  if value not in ['0', '1']:
    raise ValueError("Invalid value of record.")
  return value == '1'


def main():
  first_line = sys.stdin.readline()
  key, value = first_line.rstrip().split('\t')
  is_any_visited = is_visited(value)

  for line in sys.stdin:
    _, value = line.rstrip().split('\t')
    is_any_visited |= is_visited(value)

  value = '1' if is_any_visited else '0'
  print(f"{key}\t{value}")


if __name__ == "__main__":
  main()
