#!/usr/bin/python

#-*- coding: utf-8 -*-
#   ------------------------------------------------------------------------- /
#   wlab_datap: main.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /
import os
import json
import time
import logging
import traceback
from globals import Globals
from dataprovider import DataProvider
from mqttcatcher import MqttCatcher

from ipc import IPC_Server

if os.path.exists(Globals.RELEASE_CONFIG_FILE):
    config_f = open(Globals.RELEASE_CONFIG_FILE, 'r')
    Config = json.load(config_f)
    config_f.close()
else:
    Config = {
        "logging_path": "../../log/",
        "database_path": "../../database/",
        "socket_file_path": "../sock",
        "mqtt": [
            ('mqtt.broker.com', 1883, "", 331),
            ('mqtt.broker_another_one.com', 1883, "/topic/prefix", 331)
        ]
    }

if not os.path.exists(Config['logging_path']):
    os.mkdir(Config['logging_path'])
if not os.path.exists(Config['database_path']):
    os.mkdir(Config['database_path'])
    
logging.basicConfig(
    filename = os.path.join(Config['logging_path'], 'dataprovider.log'), 
    format = '%(asctime)s - %(name)-24.24s - %(levelname)8s - %(message)s', 
    atefmt = '%m/%d/%Y %I:%M:%S %p'
)

logger = logging.getLogger('wlabdatap')
logger.setLevel(logging.INFO)

EXIT_CODE = Globals.EXIT_CODE_EXIT

class DatabaseBot(object):
    ''' DatabaseBot '''
    
    def __init__(self, _config):
        self.logger = logger
        self.logger.critical('Object up: %s' % str(self))
        
        self.__data_prvider = DataProvider(_config['database_path'])
        self.__cmd_server = IPC_Server(_config['socket_file_path'])
        self.__cmd_server.register_cmd('GET_DESC', self.cmd_desc)
        self.__cmd_server.register_cmd('GET_MONTHLY', self.cmd_monthly_data)
        self.__cmd_server.register_cmd('GET_YEARLY', self.cmd_yearly_data)
        self.__cmd_server.register_cmd('GET_DAILY', self.cmd_daily_data)
        self.__cmd_server.register_cmd('GET_DATATREE', self.cmd_datatree)
        self.__cmd_server.register_cmd('GET_NEWEST', self.cmd_newest)
        self.__cmd_server.register_cmd('SET_SAMPLE', self.cmd_store_sample)
        self.__cmd_server.register_cmd('SET_DESC', self.cmd_station_register)
        
        # Command registred, just start
        self.__cmd_server.start()
        
        self.dataCatch = []
        for broker in _config['mqtt']:
            catcher = MqttCatcher(_config['socket_file_path'],
                                  broker[0], broker[1], 
                                  _topic_prefix=broker[2],
                                  _protocol=broker[3])
            catcher.start()
            self.dataCatch.append(catcher)
        
    def cmd_station_register(self, _json_param):
        logger.info('cmd_station_register()')
        param = json.loads(_json_param)
        self.logger.info('Got register data: %s' % str(param))
        self.__data_prvider.stationRegister(param['uid'], param['desc'])
        return json.dumps('OK')
    
    def cmd_store_sample(self, _json_param):
        logger.info('cmd_store_sample()')
        param = json.loads(_json_param)
        self.logger.info('Got sample data: %s' % str(param))
        self.__data_prvider.stationSampleStore(param['sample'])
        return json.dumps('OK')
    
    def cmd_monthly_data(self, _json_param):
        logger.info('cmd_monthly_data()')
        param = json.loads(_json_param)
        req_data = self.__data_prvider.getSerieMonthlyData(
                        param["uid"], param["serie"], param["date"])
        return json.dumps(req_data)
    
    def cmd_yearly_data(self, _json_param):
        logger.info('cmd_yearly_data()')
        param = json.loads(_json_param)
        req_data = self.__data_prvider.getSerieYearlyData(
                        param["uid"], param["serie"], param["date"])
        return json.dumps(req_data)
    
    def cmd_daily_data(self, _json_param):
        logger.info('cmd_daily_data()')
        param = json.loads(_json_param)
        req_data = self.__data_prvider.getSerieDayData(
                        param["uid"], param["serie"], param["date"])                                    
        return json.dumps(req_data)
    
    def cmd_datatree(self, _json_param):
        logger.info('cmd_datatree()')
        req_data = self.__data_prvider.getStationsDateTree()
        return json.dumps(req_data)
    
    def cmd_newest(self, _json_param):
        logger.info('cmd_newest()')
        req_data = self.__data_prvider.getStationsNewest()
        return json.dumps(req_data)
    
    def cmd_desc(self, _json_param):
        logger.info('cmd_desc()')
        req_data = self.__data_prvider.getStationsDesc()
        return json.dumps(req_data)
    
    def proc(self):
        logger.info('proc()')
        while True:
            time.sleep(1)
            
if __name__ == '__main__':
    logger.critical('Startup path: %s' % str(os.getcwd()))
    logger.critical('Version: %s' %  Globals.WLAB_VERSION)
    
    try:
        app = DatabaseBot(Config)
        app.proc()
    except:
        logger.exception(traceback.format_exc())
        EXIT_CODE = Globals.EXIT_CODE_EXCEPTION
        
    time.sleep(1)
    os._exit(EXIT_CODE)
    
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
