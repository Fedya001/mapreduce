#!/usr/bin/env bash

awk -F"\t" 'BEGIN{OFS="\t"} {name = $1; count += $2} END{print $1, count}'
