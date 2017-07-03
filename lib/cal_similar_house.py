#coding: utf-8

"""
/**
 * @file cal_similar_house.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/06/25 17:28:18
 * 例行化任务,每周日早上跑
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
import redis
import traceback

rc = Redis_client(conf.cluster_redis_conn)
print rc.rc

def str2float(item):
    if type(item) == "str":
        if item == "NULL":
            return 0
    return float(item)

def load_showing_data():
    file_obj = conf.showing_file # 文件里有本周的所有房源的带看次数,按天记录,有重复house_code
    showing_dict = {}

    for line in open(file_obj, 'r'):
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        showing = int(tmp[1])
        if house_code.startswith("BJ"):
            house_code = "1010" + house_code[-8:]
        if house_code in showing_dict:
            showing_dict[house_code] += showing
        else:
            showing_dict[house_code] = showing
    return showing_dict

# 成交房源的汇总信息
def deal_house_this_week(showing_dict):
    file_obj = open(conf.deal_house, 'r')
    sold_house_dict = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        deal_time = tmp[2]
        realmoney = tmp[3]
        build_size = tmp[5]
        create_time = tmp[6]
        if str2float(realmoney) == 0 or str2float(build_size) == 0:
            continue
        if house_code not in sold_house_dict:
            sold_house_dict[house_code] = {}

        sold_house_dict[house_code]["realmoney"] = realmoney # 成交价
        sold_house_dict[house_code]["build_area"] = build_size # 面积
        sold_house_dict[house_code]["deal_time"] = deal_time
        time_tmp = sold_house_dict[house_code]["deal_time"]
        create_time_tmp = datetime.strptime(create_time,'%Y-%m-%d %H:%M:%S')
        deal_date = datetime.strptime(time_tmp, "%Y%m%d")
        sold_house_dict[house_code]["sold_interval"] = int((deal_date - create_time_tmp).days) # 成交周期
        showing_code = house_code
        if showing_code.startswith("BJ"):
            showing_code = "1010" + house_code[-8:]
        if showing_code in showing_dict:
            sold_house_dict[house_code]["showing"] = showing_dict[showing_code] # 带看
        else:
            sold_house_dict[house_code]["showing"] = 0
    return sold_house_dict

def get_index_from_redis():
    '''
    针对每一房源获取其对应的聚类,并将房源信息落地到文件中.
    '''
    # rc = Redis_client(conf.cluster_redis_conn)
    index_file = open(conf.index_file, 'w')
    key_set = rc.get_keys('yzd_cluster_2_houses_')
    for key_item in key_set:
        tmp_cls_info = list(rc.rc.smembers(key_item))
        tmp_dict = {}
        tmp_dict['key'] = key_item
        tmp_dict['value'] = tmp_cls_info
        index_file.write(json.dumps(tmp_dict))
        index_file.write("\n")
    index_file.close()

def load_redis():
    cluster_dict = {}
    file_obj = open(conf.index_file, 'r')
    for line in file_obj:
        line = line.strip()
        tmp_dict = json.loads(line)
        key = tmp_dict['key']
        value = tmp_dict['value']  #value的结构是怎样的?
        for item in value:
            item = eval(item)
            r_id = item[1]
            cls_id = int(key.replace('yzd_cluster_2_houses_', ''))  # 将key转成簇id
            if not r_id in cluster_dict:
                cluster_dict[r_id] = []
            cluster_dict[r_id].append((item[0], cls_id, item[2], item[3]))
            # "('106100235970', '1611059976050', '86', '0')"
    return cluster_dict

def get_house_info(rc, house_code):
    '''
    {'resblock': '2511052024333', 'cluster_id': '13', 'feature_vec': '1.7,0,2,2,1,1,0,9.77,0,0,1,3,4,0,0,4,1'}
    '''

    house_code = 'yzd_house_2_cluster_' + house_code
    house_info = rc.rc.hgetall(house_code)
    cls_id = ''
    fea_list = ''
    resblock_id = ''
    if 'cluster_id' in house_info:
        cls_id = house_info['cluster_id']
    if 'feature_vec' in house_info:
        fea_list = house_info['feature_vec']
    if 'resblock' in house_info:
        resblock_id = house_info['resblock']
    fea_list_arry = fea_list.split(',')
    if cls_id != '' and len(fea_list_arry) > 0 and resblock_id != '':
        return (cls_id, fea_list_arry, resblock_id)
    else:
        return None

def get_neighbour_res(rc, resblock_id, dist):
    resblock_id = 'yzd_resblock_neighbor_' + resblock_id
    neighbour_res = rc.rc.zrangebyscore(resblock_id, 0, dist)
    r_set = set()
    for item in neighbour_res:
        item = str(item)
        cur_rid = item.split('_')[0]
        if cur_rid != resblock_id:
            r_set.add(cur_rid)
    return r_set

def get_recall_houses(rc, cls_id, res_set):
    '''
    返回相似房源
    :param rc: redis client
    :param cls_id: cluster id
    :param res_set: result set
    :return: similar houses
    '''
    h_set = set()
    cls_id = int(cls_id)
    try:
        for r_item in res_set:
            if r_item in rc:
                cur_h_set = rc[r_item]
            else:
                continue
            for h_item in cur_h_set:
                cur_h_cls = int(h_item[1])
                if cur_h_cls == cls_id:
                    h_set.add((h_item[0], h_item[2], h_item[3], r_item))
        return h_set
    except:
        return None

# def get_similar_house(index_dict, house_code):
def get_similar_house(index_dict, house_code, res_self):
    try:
        tup = get_house_info(rc, house_code)
        if tup is None:
            print house_code, "fail to get house info"
            return []
        cls_id = tup[0]
        resblock_id = tup[2]
        resblock_set = get_neighbour_res(rc, resblock_id, 1)
        resblock_set.add(res_self)
        similar_list = get_recall_houses(index_dict, cls_id, resblock_set)
        #print similar_list
        house_list = []
        for item in similar_list:
            house_list.append(item[0])
        return house_list
    except Exception, e:
        traceback.print_exc()
        return []

def similar_house_this_week(house_code, sold_house_dict, sold_similar_list):
    '''
    :param house_code: 房屋id
    :param sold_house_dict: 本周成交房源信息
    :param sold_similar_list: 该房屋id的相似成交房源列表,依赖于house_code.
    :return: 按带看量排序的相似房源列表 [房源id, 面积, 带看量, 成交周期]
    '''
    result_dict = {}
    today = (datetime.now() - timedelta(days = (1+conf.time_delta))).strftime("%Y%m%d")
    key = house_code + "_" + today
    result_dict[key] = {}
    if len(sold_similar_list) > 0:
        result_dict[key]["sold_similar"] = []
        try:
            for house in sold_similar_list:
                build_size = sold_house_dict[house]["build_area"]
                showing = sold_house_dict[house]["showing"]
                deal_interval = sold_house_dict[house]["sold_interval"]
                result_dict[key]["sold_similar"].append((house, build_size, showing, deal_interval))
            result_dict[key]["sold_similar"].sort(key=lambda x:x[2], reverse = True)  # 按照带看量大小排序
        except Exception, e:
            traceback.print_exc()
    return result_dict

def weekly_report(sold_house_dict):
    file_obj = open(conf.list_house, 'r') # 当前挂牌房源
    output_obj = open(conf.output, 'w')
    index_dict = load_redis()
    try:
        for line in file_obj:
            content = line.rstrip('\n').split('\t')
            house_code = content[0]
            resblock_id = content[5]
            if house_code.startswith('BJ'):
                house_code = '1010' + house_code[-8:]

            # similar_list可能为空
            similar_list = get_similar_house(index_dict, house_code, resblock_id) # 当前挂牌房源的相似房源
            print("get similar list")

            sold_similar_list = []
            # 判断每个挂牌房源的相似房源中是否有已成交房源
            for house in similar_list:
                if house in sold_house_dict and house != house_code:
                    sold_similar_list.append(house)
                else:
                    continue

            result_dict = similar_house_this_week(house_code, sold_house_dict, sold_similar_list)
            output_obj.write(json.dumps(result_dict))
            output_obj.write('\n')
            print("output.txt has been writen!")
    except Exception, e:
        traceback.print_exc()
    finally:
        output_obj.close()
        file_obj.close()
'''
# 老板上传相似房源数据到redis
def dump_data_to_redis():
    result = "result/" + conf.day + "/output.txt"
    yzd_weekly_report_push_key = 'yzd_weekly_report_msg_queue'
    print result

    for line in open(result, 'r'):
        tmp_dict = json.loads(line)
        key = tmp_dict.keys()
        rc.rc.lpush(yzd_weekly_report_push_key, key[0])
        hhot_redis_key = 'yzd_weekly_report_' + key[0]
        rc.rc.set(hhot_redis_key, json.dumps(tmp_dict), ex=conf.exp_time)
'''

if __name__ == "__main__":
    # pt = datetime.today() - timedelta(days=1)
    try:
        get_index_from_redis()
        showing_dict = load_showing_data()  # 房源id --> 带看数
        sold_house_dict = deal_house_this_week(showing_dict)  # 成交房源信息:成交价/面积/成交时间/签约时间/成交周期/带看次数
        weekly_report(sold_house_dict) # 将相似房源结果输出到 result/day/output.txt中
    except Exception, e:
        traceback.print_exc()
