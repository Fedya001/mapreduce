# In-Memory MapReduce

In-memory implementation of MapReduce algorithm. There are two modes: map and reduce.
In map mode code splits up the task and runs mappers in child processes. In reduce — 
it groups all records by keys and runs reducer per each key.

`Boost.Process` is used for executing child processes.

## CLI
CLI was created with `Boost.Program_options`.
```
Usage mapreduce [MODE] [OPTIONS].

Runs specified script either as a mapper or as a reducer.
There are 2 modes for specifying it: map and reduce.
Please, note that only one mode must be specified at a time.
Both scripts must work with tsv key-value files.
Additionally with option --count you can specify how many
mappers to run. Option has no effect with reduce mode.
For each unique key separate reducer is run.

Arguments:
  -h [ --help ]            display this help
  --map                    setup map mode
  --reduce                 setup reduce mode
  -s [ --script_path ] arg path to map/reduce script
  -i [ --src_file ] arg    tsv input file
  -o [ --dst_file ] arg    tsv output file
  -c [ --count ] arg (=1)  number of mappers to run
```

## Sample mappers and reducers

`samples/word_count_bash/` contains `mapper.sh` and `reducer.sh` scripts,
which solve word count problem, i.e. we are given a tsv `key`-`value` file and
we want to count number of occurences of each word in all `value`s (words are split by space).
