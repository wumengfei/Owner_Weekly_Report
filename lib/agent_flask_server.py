# coding:utf-8

"""
/**
 * @file agent_flask_server.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/07/03 18:28:18
 * 业主端周报, 每周日生成 [上周日,本周六] 的热度数据和相似房源数据
 **/
"""

from __future__ import division
from flask import Flask
from flask import request
import json
from datetime import datetime, timedelta
import sys
sys.path.append('conf')
import conf
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import redis
import numpy as np
import pdb
import traceback
from redis_client import *
# from cal_hot_data import *

app = Flask(__name__)

def get_hot_info(info_dict, house_date):
    house_code = house_date.split("_")[0]
    report_date = house_date.split("_")[1]
    this_date = datetime.strptime(report_date, "%Y%m%d")
    last_date = (this_date - timedelta(days=7)).strftime("%Y%m%d")

    #模型中的特征,存放在redis中,进行获取
    redis_info = conf.redis_conn_info
    redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])

    #pdb.set_trace()
    # 获取该套房源的关注/浏览/带看数据
    this_hot_key = "yzd_house_hot_" + house_date
    last_hot_key = "yzd_house_hot_" + house_code + '_' + last_date
    print redis_conn.get(this_hot_key)
    print redis_conn.get(last_hot_key)

    this_hot_info = eval(str(redis_conn.get(this_hot_key)))
    last_hot_info = eval(str(redis_conn.get(last_hot_key)))
    if this_hot_info == None:
        info_dict["data"]["error_msg"] += "loss hot data this week"
        return 0
    info_dict["data"]["view_this_week"] = np.sum(this_hot_info["view_nums"])
    info_dict["data"]["showing_this_week"] = np.sum(this_hot_info["show_nums"])
    info_dict["data"]["follow"] = this_hot_info["follow_nums"]

    if last_hot_info == None:
        info_dict["data"]["error_msg"] += "loss hot data last week"
        return 0
    print type(this_hot_info)
    info_dict["data"]["view_last_week"] = np.sum(last_hot_info["view_nums"])
    info_dict["data"]["showing_last_week"] = np.sum(last_hot_info["show_nums"])


def get_similar_info_homepage(result_dict, similar_dict):
    list_price = 0
    unit_price = 0
    similar_house_deal_list = []

    if similar_dict.has_key("data"):
        list_price = similar_dict["data"][0]
        build_size = similar_dict["data"][1]
        unit_price = float(list_price)/float(build_size)
    else:
        result_dict["data"]["error_msg"] += "no list price !"
    result_dict["data"]["list_price"] = list_price
    result_dict["data"]["unit_price"] = unit_price

    if similar_dict.has_key("similar_sold_list"):
        similar_house_deal_list = similar_dict["similar_sold_list"]
    else:
        result_dict["data"]["error_msg"] += "no similar house information !"
    result_dict["data"]["similar_house_deal_list"] = similar_house_deal_list

def get_similar_sold_list(info_dict, similar_dict):
    similar_house_deal_list = []
    if similar_dict.has_key("similar_sold_list"):
        similar_house_deal_list = similar_dict["similar_sold_list"]
    else:
        info_dict["data"]["error_msg"] += "no similar house information !"
    info_dict["data"]["similar_house_deal_list"] = similar_house_deal_list


@app.route('/yzd_weekly_report_similar_sold_list')
def yzd_weekly_similar_sold_list():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''

    info_dict = {}
    info_dict['data'] = {}
    info_dict['data']['error_code'] = 0
    info_dict['data']['error_msg'] = ''

    redis_similar_key = "yzd_weekly_report_" + house_date
    try:
        redis_info = conf.agent_redis_conn
        redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])
        similar_house = json.loads(redis_conn.get(redis_similar_key))
        get_similar_sold_list(info_dict, similar_house[house_date])
    except Exception, e:
        info_dict['data']['error_msg'] += 'can not get similar information;'
        info_dict["data"]["similar_house_deal_list"] = []
        info_dict["data"]["error_code"] = 1
        traceback.print_exc()
    return json.dumps(info_dict)

@app.route('/yzd_weekly_report_homepage')
def get_yzd_weekly_report():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''
    #house_date = '101092295963_20170624'
    redis_similar_key = "yzd_weekly_report_" + house_date

    info_dict = {}
    info_dict['data'] = {}
    info_dict['data']['error_code'] = 0
    info_dict['data']['error_msg'] = ''

    # 获取经纪人端周报的接口数据
    try:
        redis_info = conf.agent_redis_conn
        redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])
        similar_house = json.loads(redis_conn.get(redis_similar_key))
        print ("similar_house: ", similar_house)
        get_similar_info_homepage(info_dict, similar_house[house_date])
        print "get agent weekly report!"
    except Exception, e:
        info_dict["data"]['error_code'] = 1
        info_dict["data"]['error_msg'] += "can not get list or similar information; "
        info_dict["data"]["list_price "] = 0
        info_dict["data"]["unit_price"] = 0
        info_dict["data"]["similar_house_deal_list"] = []
        traceback.print_exc()

    # 获取热度数据
    try:
        get_hot_info(info_dict, house_date)
        print "get hot data!"
    except Exception, e:
        info_dict['data']['error_code'] = 1
        info_dict['data']['error_msg'] += "can not get hot information;"
        info_dict["data"]["view_this_week"] = 0
        info_dict["data"]["view_last_week"] = 0
        info_dict["data"]["showing_this_week"] = 0
        info_dict["data"]["showing_last_week"] = 0
        info_dict["data"]["follow"] = 0
        traceback.print_exc()

    return json.dumps(info_dict)

if __name__ == '__main__':
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(9014, '0.0.0.0')
    http_server.start(num_processes=0)
    IOLoop.instance().start()
