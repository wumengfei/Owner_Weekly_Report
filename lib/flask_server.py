# coding:utf-8

"""
/**
 * @file flask_server.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/06/22 20:05:18
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
    this_hot_info = eval(str(redis_conn.get(this_hot_key)))
    last_hot_info = eval(str(redis_conn.get(last_hot_key)))
    if this_hot_info == None or last_hot_info == None:
        print "no key"
    print type(this_hot_info)
    info_dict["data"]["view_list_of_this_week"] = this_hot_info["view_nums"]
    info_dict["data"]["total_view_num_of_this_week"] = np.sum(this_hot_info["view_nums"])
    info_dict["data"]["showing_list_of_this_week"] = this_hot_info["show_nums"]
    info_dict["data"]["total_showing_num_of_this_week"] = np.sum(this_hot_info["show_nums"])
    info_dict["data"]["view_list_of_last_week"] = last_hot_info["view_nums"]
    info_dict["data"]["total_view_num_of_last_week"] = np.sum(last_hot_info["view_nums"])
    info_dict["data"]["showing_list_of_last_week"] = last_hot_info["show_nums"]
    info_dict["data"]["total_showing_num_of_last_week"] = np.sum(last_hot_info["show_nums"])
    info_dict["data"]["follow_this_week"] = this_hot_info["follow_nums"]
    info_dict["data"]["follow_last_week"] = last_hot_info["follow_nums"]
    view_percent = 0 if np.sum(last_hot_info["view_nums"])==0 else \
        round((np.sum(this_hot_info["view_nums"])-np.sum(last_hot_info["view_nums"]))/np.sum(last_hot_info["view_nums"]),1)
    showing_percent = 0 if np.sum(last_hot_info["show_nums"])==0 else \
        round((np.sum(this_hot_info["show_nums"])-np.sum(last_hot_info["show_nums"]))/np.sum(last_hot_info["show_nums"]),1)
    follow_percent = 0 if last_hot_info["follow_nums"]==0 else round((this_hot_info["follow_nums"]-last_hot_info["follow_nums"])/last_hot_info["follow_nums"],2)
    info_dict["data"]["view_percent"] = view_percent
    info_dict["data"]["showing_percent"] = showing_percent
    info_dict["data"]["follow_percent"] = follow_percent

@app.route('/yzd_weekly_report_homepage')
def get_yzd_weekly_report():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''
    #house_date = '101092295963_20160627'
    redis_similar_key = "yzd_weekly_report_" + house_date
    house_code = house_date.split("_")[0]
    report_date = house_date.split("_")[1]

    info_dict = {}
    info_dict['data'] = {}
    info_dict['data']['error_code'] = 0
    info_dict['data']['error_msg'] = ''

    # 获取相似房源数据
    try:
        similar_house =
    except Exception, e:
        info_dict["data"]['error_code'] = 1
        info_dict["data"]['error_msg'] += "can not get similar information;"
        info_dict["data"]["similar_house_deal_num "] = 0
        info_dict["data"]["similar_house_deal_avg_showing_time"] = 0
        info_dict["data"]["similar_house_deal_avg_deal_circle"] = 0
        info_dict["data"]["similar_house_deal_list"] = []
        info_dict["data"]["similar_house_list_new_add_num"] = 0
        info_dict["data"]["similar_house_list_all_on_sale"] = 0
        info_dict["data"]["similar_house_list_avg_list_price"] = 0
        info_dict["data"]["similar_house_list_list"] = []
        info_dict["data"]["similar_house_price_up_num"] = 0
        info_dict["data"]["similar_house_price_up_percent"] = 0
        info_dict["data"]["similar_house_price_down_num"] = 0
        info_dict["data"]["similar_house_price_down_percent"] = 0
        traceback.print_exc()

    # 获取热度数据
    try:
        print "get hot data!"
        #hot_dict = json.loads(rc.rc.get(redis_hot_key))
        get_hot_info(info_dict, house_date)
    except Exception, e:
        info_dict['data']['error_code'] = 1
        info_dict['data']['error_msg'] += "can not get hot information;"
        info_dict["data"]["total_view_num_of_this_week"] = 0
        info_dict["data"]["view_list_of_this_week"] = []
        info_dict["data"]["total_view_num_of_last_week"] = 0
        info_dict["data"]["view_list_of_last_week"] = []
        info_dict["data"]["total_showing_num_of_this_week"] = 0
        info_dict["data"]["view_percent"] = 0
        info_dict["data"]["showing_list_of_this_week"] = []
        info_dict["data"]["total_showing_num_of_last_week"] = 0
        info_dict["data"]["showing_list_of_last_week"] = []
        info_dict["data"]["showing_percent"] = 0
        traceback.print_exc()
    return json.dumps(info_dict)

if __name__ == '__main__':
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(9014, '0.0.0.0')
    http_server.start(num_processes=0)
    IOLoop.instance().start()
