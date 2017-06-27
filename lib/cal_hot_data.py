#coding: utf-8

"""
/**
 * @file cal_hot_data.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/06/22 15:58:18
 **/
"""

import sys
sys.path.append('conf')
sys.path.append('lib')
import conf
import log
from datetime import datetime, timedelta
import pandas as pd
import redis
import json


def get_file_loc(time_point):
    '''根据time_point时间点, 计算一周前(7天)的日期列表
    Args:
        time_point: datetime.datetime类型
    Return:
        day_list: list, 存放近一周的日期列表, 格式为'YYYYMMDD'
    '''

    today = time_point.strftime('%Y%m%d')
    last_day = (time_point - timedelta(days=6)).strftime('%Y%m%d')
    day_list_tmp = pd.period_range(last_day, today, freq="D")
    day_list = [item.strftime('%Y%m%d') for item in day_list_tmp]
    return day_list

def get_week_follow(time_point):
    '''计算每套房源自time_point向前推一周内的所有关注量, 计算逻辑为每套房源该周七天关注量的极大值(da表)
    Args:
        time_point: datetime.datetime类型
    Return:
        follow_dic: dict, house_code-->follow_nums
    '''

    follow_dic = dict()
    week_list = get_file_loc(time_point)
    for date_item in week_list:
        file_url = "data/%s/follow_nums.txt" % date_item
        for line in open(file_url, 'r'):
            content = line.strip().split('\t')
            house_code = content[0]
            follow_num = int(content[1])
            follow_dic.setdefault(house_code, follow_num)
            follow_dic[house_code] = max(follow_dic[house_code], follow_num)
    return follow_dic

def get_week_show(time_point):
    '''计算每套房源自time_point向前推一周内的所有带看量, 并以列表的形式返回7天的数据
    Args:
        time_point: datetime.datetime类型
    Return:
        follow_dic: dict, house_code-->[show_nums] (按照时间顺序)
    '''

    show_dic = dict()
    week_list = get_file_loc(time_point)
    index = -1
    for date_item in week_list:
        index += 1
        file_url = "data/%s/show_nums.txt" % date_item
        for line in open(file_url, 'r'):
            content = line.strip().split('\t')
            house_code = content[0]
            show_num = int(content[1])
            show_dic.setdefault(house_code, [0]*7)
            show_dic[house_code][index] = show_num
    return show_dic

def get_week_view(time_point):
    '''计算每套房源自time_point向前推一周内的所有浏览量, 并以列表的形式返回7天的数据
    Args:
        time_point: datetime.datetime类型
    Return:

        follow_dic: dict, house_code-->[view_nums] (按照时间顺序)
    '''

    view_dic = dict()
    week_list = get_file_loc(time_point)
    index = -1
    for date_item in week_list:
        index += 1
        file_url = "data/%s/view_nums.txt" % date_item
        for line in open(file_url, 'r'):
            content = line.strip().split('\t')
            house_code = content[0]
            view_num = int(content[1])
            view_dic.setdefault(house_code, [0]*7)
            view_dic[house_code][index] = view_num
    return view_dic

def union_hot_data(follow_dic, view_dic, show_dic):
    '''将每套房源的三个热度数据(关注/浏览/带看)整合入字典, 为redis提供数据源
    Args:
        follow_dic, view_dic, show_dic; 其中, 关注字典中value为数字, 其余两个为列表.
    Return:

        hot_dic: dict, house_code-->["view_nums"]/["show_nums"]/["follow_nums"]
    '''

    # 之前的写法有bug!!
    # hot_file = open(conf.hot_data_all, 'w')
    hot_dic = dict()
    for house_code, num_lst in view_dic.iteritems():
        hot_dic.setdefault(house_code, {"view_nums": [0,0,0,0,0,0,0], "show_nums": [0,0,0,0,0,0,0], "follow_nums": 0})
        hot_dic[house_code]["view_nums"] = num_lst
    for house_code, num_lst in show_dic.iteritems():
        hot_dic.setdefault(house_code, {"view_nums": [0,0,0,0,0,0,0], "show_nums": [0,0,0,0,0,0,0], "follow_nums": 0})
        hot_dic[house_code]["show_nums"] = num_lst
    for house_code, nums in follow_dic.iteritems():
        hot_dic.setdefault(house_code, {"view_nums": [0,0,0,0,0,0,0], "show_nums": [0,0,0,0,0,0,0], "follow_nums": 0})
        hot_dic[house_code]["follow_nums"] = nums

    # for keys in hot_dic:
    #     hot_file.write(json.dumps({keys: hot_dic[keys]}))
    # hot_file.close()
    return hot_dic

def update_redis(redis_key, house_date, hot_dic):
    redis_info = conf.redis_conn_info
    redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])
    cnt = 0
    for key in hot_dic:
        redis_conn.set(redis_key+key+'_'+house_date, hot_dic[key])
        cnt += 1
        if cnt % 100 == 0:
            print(cnt, redis_key+key+'_'+house_date)

if __name__ == "__main__":
    pt = datetime.today() - timedelta(days=1)
    follow_dic = get_week_follow(pt)
    view_dic = get_week_view(pt)
    show_dic = get_week_show(pt)
    hot_dic = union_hot_data(follow_dic, view_dic, show_dic)
    house_date = pt.strftime('%Y%m%d')
    update_redis("yzd_house_hot_", house_date, hot_dic)
    print("hot data update at %s" % house_date)
