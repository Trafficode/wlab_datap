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
import datetime
import pytz
import struct


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
    
    def __bin2dict(self, packet):
        version1_len = 37
        packet_dict = {
            "version": ord(packet[0]) & 0x1F,
            "id": "%02X%02X%02X%02X%02X%02X" % (ord(packet[6]), ord(packet[5]), ord(packet[4]), ord(packet[3]), ord(packet[2]), ord(packet[1])),
            "ts": int(struct.unpack('<q', packet[7:15])[0]),
            "temp_act": struct.unpack('<h', packet[15:17])[0],
            "temp_avg": struct.unpack('<h', packet[17:19])[0],
            "temp_max": struct.unpack('<h', packet[19:21])[0],
            "temp_min": struct.unpack('<h', packet[21:23])[0],
            "temp_max_ts_offset": struct.unpack('<h', packet[23:25])[0],
            "temp_min_ts_offset": struct.unpack('<h', packet[25:27])[0],
            "humidity_act": ord(packet[27]),
            "humidity_avg": ord(packet[28]),
            "humidity_max": ord(packet[29]),
            "humidity_min": ord(packet[30]),
            "humidity_max_ts_offset": struct.unpack('<h', packet[31:33])[0],
            "humidity_min_ts_offset": struct.unpack('<h', packet[33:35])[0],
            "battery_voltage": struct.unpack('<h', packet[35:37])[0],
        }
        return(packet_dict, version1_len)

    def __bin2old(self, packet_dict):
        packet_wlab_dict = {
            "UID": packet_dict["id"],
            "TS": int(packet_dict["ts"]),
            "SERIE": {
                "Temperature": {
                    "f_avg": float(packet_dict["temp_avg"])/10.0,
                    "f_act": float(packet_dict["temp_act"])/10.0,
                    "f_min": float(packet_dict["temp_min"])/10.0,
                    "f_max": float(packet_dict["temp_max"])/10.0,
                    "i_min_ts": packet_dict["ts"] + packet_dict["temp_min_ts_offset"],
                    "i_max_ts": packet_dict["ts"] + packet_dict["temp_max_ts_offset"],
                },
                "Humidity": {
                    "f_avg": float(packet_dict["humidity_avg"]),
                    "f_act": float(packet_dict["humidity_act"]),
                    "f_min": float(packet_dict["humidity_min"]),
                    "f_max": float(packet_dict["humidity_max"]),
                    "i_min_ts": packet_dict["ts"] + packet_dict["humidity_min_ts_offset"],
                    "i_max_ts": packet_dict["ts"] + packet_dict["humidity_max_ts_offset"],
                }
            }
        }
        return(packet_wlab_dict)

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
            if msg.topic == self.AuthTopic:
                _data = json.loads(str(msg.payload))
                _param = {
                    'uid': _data['uid'],
                    'desc': _data
                    }
                ipc_send_receive(self.IpcSockPath, 
                                 'SET_DESC', 
                                 json.dumps(_param),
                                 2000)
            elif msg.topic == self.SampleTopic:
                _data = json.loads(str(msg.payload))
                _param = {
                    'sample': _data
                    }
                ipc_send_receive(self.IpcSockPath,
                                 'SET_SAMPLE', 
                                 json.dumps(_param),
                                 2000)
            elif msg.topic == self.SampleBinTopic:
                _samples_n = 1 + (ord(msg.payload[0]) >> 5)
                _offset = 0
                for _ in range(_samples_n):
                    _bin_data_dict, _sample_len = self.__bin2dict(msg.payload[_offset:])
                    _offset += _sample_len
                    _param = {
                        'sample': self.__bin2old(_bin_data_dict)
                        }
                    ipc_send_receive(self.IpcSockPath,
                                    'SET_SAMPLE', 
                                    json.dumps(_param),
                                    2000)
            else:
                self.logger.error('Received message to unknown topic')
        except:
            self.logger.exception('Exception in on_message:\n%s' % \
                                    traceback.format_exc())
                            
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
