#!/bin/bash

for DATASET in 1season 5season 20season CombinatedDataset/1season CombinatedDataset/5season CombinatedDataset/20season
do
  echo "--- Running test for dataset ${DATASET} ---"
  bash run.sh $DATASET
done
