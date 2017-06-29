#coding: utf-8
"""
/**
 * @file redis_client.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/06/26 12:18:18
 *
 **/
"""
import os
import sys
import urllib2
import re
import time
import hashlib
import json
import redis

class Redis_client(object):
    """
    配置redis client
    """
    def __init__(self, redis_info):
        try:
            pool = redis.ConnectionPool( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"],\
                              password = redis_info["redis_pwd"], socket_connect_timeout = redis_info["redis_con_timeout"],\
                              socket_timeout = redis_info["redis_trans_timeout"])
            self.rc = redis.StrictRedis(connection_pool=pool)
        except:
            print("failed to connect redis server!")
            exit(1)

        self.pipe = self.rc.pipeline()
        if self.pipe == None:
            print("failed to get redis client pipeline!")
            exit(1)

    def __del__(self):
        pass

    def get_keys(self, key_pre):
        # 返回包含key_pre前缀的所有key的集合
        keys = self.rc.keys()
        rt_keys = set()
        for item in keys:
            if item.find(key_pre) != -1:
                # 加log
                rt_keys.add(item)
        return rt_keys

    def erase_kv(self, keys):
        if len(keys) == 0:
            return
        for key in keys:
            self.rc.config_set('stop-writes-on-bgsave-error', 'no')
            self.rc.delete(key)
