#!/usr/bin/env bash

: '
  This script accepts the following arguments in strict order:
  * mapreduce framework binary;
  * mapper script;
  * reducer script;
  * input file (the input data is stored here);
  * output file (the final script result would be here);
  * --depth [number] option.

  And it runs a web-crawler, which visits all urls at `number` depth or less.
'

handle_error() {
  echo "$1"
  echo "Usage: ./web_crawler.sh mapreduce_binary mapper reducer input output --depth [number]."
  exit 1
}

if [ $# -ne 7 ]; then
  handle_error "Invalid number of parameters."
fi

if [ "$6" != "--depth" ]; then
  handle_error "Invalid option."
fi

number_regex='^[+-]?[1-9][0-9]*$'
if ! [[ "$7" =~ $number_regex ]]; then
  handle_error "Invalid number format."
fi

for ((i=0; i < $7; i++)); do
  echo "[depth = $((i + 1))]"
  # Run at most 10'000 mappers.
  "$1" --map -s "$2" -i "$4" -o "$5" -c 10000
  mv "$5" "$4"
  echo "[OK] map stage"

  "$1" --reduce -s "$3" -i "$4" -o "$5"
  mv "$5" "$4"
  echo "[OK] reduce stage"
done

# Rename input to output.
mv "$4" "$5"
