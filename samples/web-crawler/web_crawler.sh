#!/usr/bin/env bash

: '
  This script accepts the following arguments in strict order:
  * mapreduce framework binary; #1
  * mapper script; #2
  * reducer script; #3
  * final reducer sript; #4
  * input file (the input data is stored here); #5
  * output file (the final script result would be here); #6
  * --depth [number] option. #7, #8

  And it runs a web-crawler, which visits all urls at `number` depth or less.
'

handle_error() {
  echo "$1"
  echo "Usage: ./web_crawler.sh mapreduce_binary mapper reducer final_reducer input output --depth [number]."
  exit 1
}

if [ $# -ne 8 ]; then
  handle_error "Invalid number of parameters."
fi

if [ "$7" != "--depth" ]; then
  handle_error "Invalid option."
fi

number_regex='^[+-]?[1-9][0-9]*$'
if ! [[ "$8" =~ $number_regex ]]; then
  handle_error "Invalid number format."
fi

for ((i=0; i < $8; i++)); do
  echo "[depth = $((i + 1))]"
  # Run at most 400 mappers.
  "$1" --map -s "$2" -i "$5" -o "$6" -c 400
  mv "$6" "$5"
  echo "[OK] map stage"

  "$1" --reduce -s "$3" -i "$5" -o "$6"
  mv "$6" "$5"
  echo "[OK] reduce stage"
done

# Run final reducer to get a clean list of urls.
"$1" --reduce -s "$4" -i "$5" -o "$6"
