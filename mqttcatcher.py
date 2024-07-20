#-*- coding: utf-8 -*-
#   ------------------------------------------------------------------------- /
#   wlab_datap: mqttcatcher.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /
import time
import json
import logging
import traceback
import threading
from globals import Globals 
from ipc import ipc_send_receive
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv311, MQTTv31

class MqttCatcher(object):
    ''' MqttCatcher '''
    WLAB_AUTH_TOPIC='/wlabauth'
    WLAB_DB_TOPIC='/wlabdb'
    WLAB_DB_BIN_TOPIC='/wlabdb/bin'

    def __init__(self,  _ipc_sock,
                        _mqtt_broker, 
                        _mqtt_port, 
                        _topic_prefix='',   # /test/meteo 
                        _protocol=311):     # 31 or 311
        
        self.logger = logging.getLogger('mqtt_'+str(_mqtt_broker))
        self.logger.critical('Object up: %s' % str(self))
        
        self.IpcSockPath = _ipc_sock
        self.AuthTopic = _topic_prefix + self.WLAB_AUTH_TOPIC
        self.SampleTopic = _topic_prefix + self.WLAB_DB_TOPIC
        self.SampleBinTopic = _topic_prefix + self.WLAB_DB_BIN_TOPIC

        if _protocol == 31:
            proto = MQTTv31
        else:
            proto = MQTTv311
            
        self.mqtt_broker = _mqtt_broker
        self.mqtt_port = _mqtt_port
        self.client = mqtt.Client(
            client_id='', 
            clean_session=True, 
            userdata=None, 
            protocol=proto
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.proc_tid = threading.Thread(
            name='mqtt_thread', 
            target=self.__proc
        )
    
    def start(self):
        self.logger.critical('start()')
        while True:
            try:
                self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
                self.proc_tid.start()
                break
            except:
                self.logger.exception('Connection failed, try again...')
                time.sleep(5)
            
    def __proc(self):
        self.logger.critical('__proc()')
        try:
            while True:
                self.client.loop_forever()
        except:
            self.logger.exception('Exception in __proc:\n%s' % \
                                    traceback.format_exc())
            
    def on_connect(self, client, userdata, flags, rc):
        self.logger.critical('on_connect()')
        try:
            self.client.subscribe(self.WLAB_AUTH_TOPIC)
            self.client.subscribe(self.WLAB_DB_TOPIC)
            self.client.subscribe(self.WLAB_DB_BIN_TOPIC)
        except:
            self.logger.exception('Exception in on_connect:\n%s' % \
                                    traceback.format_exc())
            
    def on_message(self, client, userdata, msg):
        try:
            _data = json.loads(str(msg.payload))            
            if msg.topic == self.AuthTopic:
                _param = {
                    'uid': _data['uid'],
                    'desc': _data
                    }
                ipc_send_receive(self.IpcSockPath, 
                                 'SET_DESC', 
                                 json.dumps(_param),
                                 2000)
            elif msg.topic == self.SampleTopic:
                _param = {
                    'sample': _data
                    }
                ipc_send_receive(self.IpcSockPath,
                                 'SET_SAMPLE', 
                                 json.dumps(_param),
                                 2000)
            elif msg.topic == self.SampleBinTopic:
                pass
            else:
                self.logger.error('Received message to unknown topic')
        except:
            self.logger.exception('Exception in on_message:\n%s' % \
                                    traceback.format_exc())
                            
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
