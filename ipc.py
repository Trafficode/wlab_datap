#-*- coding: utf-8 -*-
#   ------------------------------------------------------------------------- /
#   wlab_webapp: ipc.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /
import os
import socket
import logging
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def ipc_send_receive(_sock_file, _cmd, _str, _timeout=0.1):
    _answer = ''
    try:
        _sock = None        
        _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        _sock.connect(_sock_file)        
        _send_data = _cmd+' '+_str+'\0'
        logger.info('Send len: %d' % len(_send_data))
        _sock.sendall(_send_data)
        _sock.settimeout(_timeout)
        while True:
            _data = _sock.recv(1024)
            if _data:
                _answer += _data
            else:
                # when connection closed on server side, empty string recv
                break
    except Exception as e:
        logger.error('ipc_send_receive, Exception: %s' % str(e))
    finally:
        if _sock:
            _sock.close()
    return(_answer)

class IPC_Server(threading.Thread):
    ''' IPC_Server '''
    
    def __init__(self, _sock_file):
        super(IPC_Server, self).__init__()
        
        self.logger = logging.getLogger('IPC_Server')
        self.logger.setLevel(logging.INFO)
        
        self.logger.critical('<IPC_Server> module started...')
        
        self.__sock_file = _sock_file
        self.__cmds = {}
        self.__running = True
        self.__done_evt = threading.Event()
        self.__done_evt.clear()
        
    def register_cmd(self, _name, _cb):
        '''
        :param
            _name: string ex. 'TEST'
            _cb: def cmd_test(str: _param)
                     return 'answer'
        '''
        self.__cmds[_name] = _cb
        
    def stop(self):
        self.logger.critical('IPC_Server.stop()')
        self.__running = False
        self.__done_evt.wait()
        self.logger.critical('IPC_Server stopped')
        
    def run(self):
        self.logger.info('IPC_Server.run start')
        if os.path.exists(self.__sock_file):
            os.unlink(self.__sock_file)
        _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        _sock.bind(self.__sock_file)
        _sock.listen(1)
        os.chmod(self.__sock_file, 0777)
        
        while self.__running:
            conn = None
            try:
                conn, addr = _sock.accept()
                while True:
                    _recv_data = ''
                    while True:
                        _data = conn.recv(1024)
                        if _data:
                            _recv_data += _data
                            if _recv_data[-1] == '\0':
                                break
                        else:
                            break
                    _answer = ''
                    _cmd, _param = _recv_data[:-1].split(' ', 1)
                    _cmd_proc = self.__cmds.get(_cmd, None)
                    if _cmd_proc:
                        _answer = _cmd_proc(_param)
                    else:
                        self.logger.error('Unknown command: %s' % str(_cmd))
                    conn.sendall(_answer)
                    break
            except Exception as e:
                self.logger.error('Excetion: %s' % str(e))
            finally:
                if conn:
                    conn.close()
                
        self.logger.info('IPC_Server.run done.')
        self.__done_evt.set()
        
# -----------------------------------------------------------------------------
#    end of file
# -----------------------------------------------------------------------------
