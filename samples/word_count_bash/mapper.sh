#!/usr/bin/env bash

while read -r line; do
  key=$(cut -f1 -d$'\t' <<< "$line")
  value=$(cut -f2 -d$'\t' <<< "$line")

  # tr -cd ' ' <<< "value" | wc -c
  while read -r -d' ' word; do
    echo "$word 1"
  done <<< "$value "
done
