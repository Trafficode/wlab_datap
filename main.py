#   ------------------------------------------------------------------------- /
#   wlab_web_dataprovider: main.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /

import os
import json
import time
import logging
import traceback
from user_config import Config
from dataprovider import DataProvider
from mqttcatcher import MqttCatcher

from ipc import IPC_Server

if Config.DEVELOP:
    log_file_path = 'log/dataprovider.log'
    db_path = 'database'
    if not os.path.exists('log'):
        os.mkdir('log')
    if not os.path.exists(db_path):
        os.mkdir(db_path)
else:
    log_file_path = '/home/wlab/weatherlab/log/dataprovider.log'
    db_path = '/home/wlab/weatherlab/db'
    
logging.basicConfig(
    filename = log_file_path, 
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    atefmt = '%m/%d/%Y %I:%M:%S %p'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EXIT_CODE = Config.EXIT_CODE_EXIT

class DatabaseBot(object):
    ''' DatabaseBot '''
    
    def __init__(self, _db_path):
        self.logger = logger
        self.logger.critical('Object up: %s' % str(self))
        
        self.__data_prvider = DataProvider(db_path)
        self.__cmd_server = IPC_Server(Config.CONFIG_IPC_DP_SERVER_PORT)
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
        
        # Catch data from stations
        self.data_catcher = MqttCatcher('194.42.111.14', 1883)
        self.data_catcher.start()
        
    def cmd_station_register(self, _json_param):
        logger.info('cmd_station_register()')
        param = json.loads(_json_param)
        self.__data_prvider.stationRegister(param['uid'], param['desc'])
        return json.dumps('OK')
    
    def cmd_store_sample(self, _json_param):
        logger.info('cmd_store_sample()')
        param = json.loads(_json_param)
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
    logger.critical('\n\n')
    logger.critical('Startup develop: %s' % str(Config.DEVELOP))
    logger.critical('Startup path: %s' % str(os.getcwd()))
    logger.critical('Version: %s' %  Config.WLAB_VERSION)
    
    try:
        app = DatabaseBot(db_path)
        app.proc()
    except:
        logger.exception(traceback.format_exc())
        EXIT_CODE = Config.EXIT_CODE_EXCEPTION
        
    time.sleep(1)
    os._exit(EXIT_CODE)
    
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
