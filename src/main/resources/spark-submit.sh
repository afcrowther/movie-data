#!/usr/bin/env bash

# Not actual settings, just an example, executors should be 3x cores available, cores assumed to be 4 per machine
# so this would be just an example for 5 nodes setup on yarn.
# If reading/ writing file from/ to hdfs you would need to specify the full path (including ip)
spark-submit \
  --class movies.Driver \
  --master yarn \
  --queue spark \
  --num-executors 60 \
  --executor-cores 4 \
  --executor-memory 5g \
  --deploy-mode client \
  /my/jar/location/movie-data-assembly-1.0.jar \
    movies/input/path.tsv

