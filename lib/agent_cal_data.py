#coding: utf-8

"""
/**
 * @file agent_cal_data.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/07/03 16:58:18
 * 经纪人端周报数据处理
 **/
"""

import sys
sys.path.append('conf')
sys.path.append('lib')
import conf
import log
from datetime import datetime, timedelta
import pandas as pd
from redis_client import *
from cal_similar_house import *
import redis
import traceback


def get_onlist_house_feature():
    '''
    当前挂牌详情
    :return: house_feature_dict:    house_code -> data -> [list_price, build_size]
                                               -> resblock_id
    '''
    data_file = conf.list_house

    file_obj = open(data_file, 'r')
    house_feature_dict = {}

    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        list_price = tmp[1]
        build_size = tmp[2]
        resblock_id = tmp[5]
        #if house_code not in ["103100336071", "101090254336", "101090161365"]:
        #    continue
        #if city_id not in city_list:
        #    continue
        if house_code not in house_feature_dict:
            house_feature_dict[house_code] = {}
            house_feature_dict[house_code]["data"] = []
            house_feature_dict[house_code]["resblock_id"] = resblock_id
        house_feature_dict[house_code]["data"].append(list_price)
        house_feature_dict[house_code]["data"].append(build_size)

    return house_feature_dict

def get_sold_list():
    '''
    本周成交详情
    :return: sold_house_dict: house_code -> [house_code, dealtime, unit_price, deal_cycle]
    '''
    sold_file_obj = open(conf.deal_house, 'r')
    sold_house_dict = {}

    for line in sold_file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        dealtime = tmp[2]
        realmoney = tmp[3]
        build_size = tmp[5]
        list_time = tmp[6]

        if house_code not in sold_house_dict:
            sold_house_dict[house_code] = []
        sold_house_dict[house_code].append(house_code)
        sold_house_dict[house_code].append(dealtime)
        try:
            unit_price = float(realmoney) / float(build_size)
        except Exception, e:
            unit_price = 0
        sold_house_dict[house_code].append(unit_price)
        deal_date = datetime.strptime(dealtime, "%Y%m%d")
        list_date = datetime.strptime(list_time,'%Y-%m-%d %H:%M:%S')
        deal_cycle = (deal_date - list_date).days
        sold_house_dict[house_code].append(deal_cycle)

    return sold_house_dict

def similar_sold_house_list(onlist_house_dict):
    '''

    :param onlist_house_dict:
    :return:
    '''
    sold_dict = get_sold_list()
    similar_sold_list = {}
    index_dict = load_redis()
    result_file = conf.result_file

    for house in onlist_house_dict:
        onlist_house_dict[house]["similar_sold_list"] = []
        resblock_id = onlist_house_dict[house]["resblock_id"]
        similar_list = get_similar_house(index_dict, house, resblock_id)
        print similar_list
        for item in similar_list:
            if item in sold_dict:
                onlist_house_dict[house]["similar_sold_list"].append(sold_dict[item])

    result_obj = open(result_file, 'w')
    for item in onlist_house_dict:
        result_dict = {}
        key = item + "_" + conf.day
        result_dict[key] = {}
        result_dict[key]["data"] = onlist_house_dict[item]["data"]
        result_dict[key]["similar_sold_list"] = onlist_house_dict[item]["similar_sold_list"]
        result_obj.write(json.dumps(result_dict) + "\n")

    return similar_sold_list

if __name__ == "__main__":
    onlist_house_dict = get_onlist_house_feature()
    similar_sold_house_list(onlist_house_dict)
