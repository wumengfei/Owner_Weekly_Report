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
# import conf
# import log
from datetime import datetime, timedelta
import pandas as pd


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
    '''计算每套房源自time_point向前推一周内的所有关注量, 计算逻辑为每套房源该周七天关注量的极大值(da表)
    Args:
        time_point: datetime.datetime类型
    Return:
        follow_dic: dict, house_code-->[show_nums] (按照时间顺序)
    '''

    show_dic = dict()
    week_list = get_file_loc(time_point)
    for date_item in week_list:
        file_url = "data/%s/show_nums.txt" % date_item
        for line in open(file_url, 'r'):
            content = line.strip().split('\t')
            house_code = content[0]
            show_num = int(content[1])
            show_dic.setdefault(house_code, [])
            show_dic[house_code].append(show_num)
    return show_dic

def get_week_view(time_point):
    '''计算每套房源自time_point向前推一周内的所有关注量, 计算逻辑为每套房源该周七天关注量的极大值(da表)
    Args:
        time_point: datetime.datetime类型
    Return:

        follow_dic: dict, house_code-->[view_nums] (按照时间顺序)
    '''

    view_dic = dict()
    week_list = get_file_loc(time_point)
    for date_item in week_list:
        file_url = "data/%s/view_nums.txt" % date_item
        for line in open(file_url, 'r'):
            content = line.strip().split('\t')
            house_code = content[0]
            view_num = int(content[1])
            view_dic.setdefault(house_code, [])
            view_dic[house_code].append(view_num)
    return view_dic

if __name__ == "__main__":
    pass