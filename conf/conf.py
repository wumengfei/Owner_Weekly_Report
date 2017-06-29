#coding: utf-8

import os
import sys
from datetime import datetime
from datetime import timedelta

now = datetime.now()
day = datetime.strftime((now - timedelta(days=1)), "%Y%m%d")
last_week = datetime.strftime((now - timedelta(days=8)), "%Y%m%d")

#redis配置参数
redis_conn_info = {
    "host": "172.30.0.20",\
    "port": 6379,\
    "db": 1
}

cluster_redis_conn = {
    "host": "10.10.9.16",\
    "port": 6379,\
    "redis_pwd": "e729647141aae4f859453d8fbabc251b",\
    "db": 5,\
    "redis_con_timeout": 30,\
    "redis_trans_timeout": 30
}

index_file = "data/redis_index.txt" # 将redis获取到的
deal_house = "data/" + day + "/" + "house_deal.txt"
list_house = "data/" + day + "/" + "house_onsale.txt"

# base on today, minus two weeks, means save two week data
#redis_save_window = datetime.strftime((now - timedelta(days=29)), "%Y%m%d")
showing_file = "data/" + day + "/" + "house_showing.txt"  # 本周带看量


output_dir = "result/" + day
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
output = output_dir + "/" + "output.txt"

exp_time = 86400 * 35 # 周报redis中key的有效时长

homepage_show = 3
