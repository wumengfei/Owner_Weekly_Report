# coding:utf-8

"""
/**
 * @file flask_server.py
 * @author wumengfei(@lianjia.com)
 * @date 2017/06/22 20:05:18
 *
 **/
"""

from __future__ import division
from flask import Flask
from flask import request
import json
from datetime import datetime, timedelta
import sys
sys.path.append('conf')
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

app = Flask(__name__)

@app.route('/yzd_weekly_report_homepage')
def get_yzd_weekly_report():
    pass


if __name__ == '__main__':
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(8015, '0.0.0.0')
    http_server.start(num_processes=0)
    IOLoop.instance().start()