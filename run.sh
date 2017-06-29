#!/bin/bash

sh bin/get_data_weekly.sh 0 0
python lib/cal_hot_data.py >> log/run.log
python lib/cal_similar_house.py >> log/run.log
python lib/dump_data_to_redis.py >> log/run.log
