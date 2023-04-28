#!/bin/bash

#load python
module load python/3.5.1

#remove the output file
hadoop fs -rm -r /user/ama336/Test/Nintendo/output/nintendo_output

# Run the MapReduce job for Nintendo
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input /user/ama336/Test/Nintendo/input/nintendo_instagram.csv \
    -output /user/ama336/Test/Nintendo/output/nintendo_output \
    -mapper "python3 MapReducers/mapper.py" \
    -reducer "python3 MapReducers/reducer.py /user/ama336/Test/Nintendo/input/nintendo_stock.csv" \
    -file MapReducers/mapper.py \
    -file MapReducers/reducer.py

#remove gopro output file
hadoop fs -rm -r /user/ama336/Test/goPro/output/gopro_output

# Run the MapReduce job for GoPro
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input /user/ama336/Test/goPro/input/gopro_instagram.csv \
    -output /user/ama336/Test/goPro/output/gopro_output \
    -mapper "python3 MapReducers/mapper.py" \
    -reducer "python3 MapReducers/reducer.py /user/ama336/Test/goPro/input/gopro_stock.csv" \
    -file MapReducers/mapper.py \
    -file MapReducers/reducer.py

#remove tesla output file
hadoop fs -rm -r /user/ama336/Test/Tesla/output/tesla_output

# Run the MapReduce job for Tesla
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input /user/ama336/Test/Tesla/input/tesla_instagram.csv \
    -output /user/ama336/Test/Tesla/output/tesla_output \
    -mapper "python3 MapReducers/mapper.py" \
    -reducer "python3 MapReducers/reducer.py /user/ama336/Test/Tesla/input/tesla_stock.csv" \
    -file MapReducers/mapper.py \
    -file MapReducers/reducer.py
