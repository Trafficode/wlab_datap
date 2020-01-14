#   ------------------------------------------------------------------------- /
#   WeatherLab mqttcatcher.py
#   $Rev: 242 $
#   $Author: kkuras $
#   $Date: 2018-01-29 23:12:01 +0100 (pon) $
#   ------------------------------------------------------------------------- /

import json
import logging
import traceback
import threading
from ipc import ipc_send_receive
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv311, MQTTv31

logger = logging.getLogger("mqtt")

class MqttCatcher(object):
    ''' MqttCatcher '''
    WLAB_AUTH_TOPIC="/wlabauth"
    WLAB_DB_TOPIC="/wlabdb"
    
    # protocol: {31, 311}
    def __init__(self, _mqtt_broker, _mqtt_port, _protocol=311):
        self.logger = logger
        self.logger.critical('Object up: %s' % str(self))
        
        if _protocol == 31:
            proto = MQTTv31
        else:
            proto = MQTTv311
            
        self.mqtt_broker = _mqtt_broker
        self.mqtt_port = _mqtt_port
        self.client = mqtt.Client(
            client_id="", 
            clean_session=True, 
            userdata=None, 
            protocol=proto
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.proc_tid = threading.Thread(
            name='send_thread', 
            target=self.__proc
        )
    
    def start(self):
        self.logger.critical("start()")
        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.proc_tid.start()
        except:
            self.logger.exception("Exception in start:\n%s" % \
                                  traceback.format_exc())
            
    def __proc(self):
        self.logger.critical("__proc()")
        try:
            while True:
                self.client.loop_forever()
        except:
            self.logger.exception("Exception in __proc:\n%s" % \
                                  traceback.format_exc())
            
    def on_connect(self, client, userdata, flags, rc):
        self.logger.critical("on_connect()")
        try:
            self.client.subscribe(self.WLAB_AUTH_TOPIC)
            self.client.subscribe(self.WLAB_DB_TOPIC)
        except:
            self.logger.exception("Exception in on_connect:\n%s" % \
                                  traceback.format_exc())
            
    def on_message(self, client, userdata, msg):
        try:
            _data = json.loads(str(msg.payload))            
            if msg.topic == self.WLAB_AUTH_TOPIC:
#                 self.station_register(_data["uid"], _data)
                _param = {
                    'uid': _data["uid"],
                    'desc': _data
                    }
                ipc_send_receive('SET_DESC', json.dumps(_param))           
            elif msg.topic == self.WLAB_DB_TOPIC:
#                 self.station_sample(_data)
                _param = {
                    'sample': _data
                    }
                ipc_send_receive('SET_SAMPLE', json.dumps(_param))
            else:
                self.logger.error("Received message to unknown topic")
        except:
            self.logger.exception("Exception in on_message:\n%s" % \
                                  traceback.format_exc())
                            
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
