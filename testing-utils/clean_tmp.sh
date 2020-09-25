#!/usr/bin/env bash

# Cleans all tmp files in current direcory.
ls | grep -E ".{4}-.{4}-.{4}-.{4}" | xargs -d"\n" rm
