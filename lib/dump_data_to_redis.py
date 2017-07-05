import json
import os
import redis_client
import sys
sys.path.append("conf")
import conf
from datetime import datetime
from datetime import timedelta
import redis

def dump_data_to_redis():
    '''
    上传业主端redis
    :return:
    '''
    redis_info = conf.redis_conn_info
    redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])

    result = "result/" + conf.day + "/output.txt"
    #yzd_weekly_report_push_key = 'yzd_weekly_report_msg_queue'
    print result
    cnt = 0

    for line in open(result, 'r'):
        tmp_dict = json.loads(line)
        key = tmp_dict.keys()
        #rc.rc.lpush(yzd_weekly_report_push_key, key[0])
        similar_redis_key = 'yzd_weekly_report_' + key[0]
        redis_conn.set(similar_redis_key, json.dumps(tmp_dict), ex=conf.exp_time)
        cnt += 1
        if cnt % 100 == 0:
            print(cnt, similar_redis_key)

def dump_agent_data_to_redis():
    '''
    上传经纪人端redis
    :return:
    '''
    redis_info = conf.agent_redis_conn
    redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])

    result = "result/" + conf.day + "/result.txt"
    print result
    cnt = 0

    for line in open(result, 'r'):
        tmp_dict = json.loads(line)
        key = tmp_dict.keys()
        #rc.rc.lpush(yzd_weekly_report_push_key, key[0])
        similar_redis_key = 'yzd_weekly_report_' + key[0]
        redis_conn.set(similar_redis_key, json.dumps(tmp_dict), ex=conf.agent_exp_time)
        cnt += 1
        if cnt % 100 == 0:
            print(cnt, similar_redis_key)

if __name__ == "__main__":
    dump_data_to_redis()
    dump_agent_data_to_redis()
