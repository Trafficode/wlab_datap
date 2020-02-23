#   ------------------------------------------------------------------------- /
#   wlab_datap: main.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /

import traceback
import threading
import socket
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def ipc_send_receive(_port, _cmd, _jstr, _timeout):
    rc = None
    try:
        # Create a TCP/IP socket, _port > 1023
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('localhost', _port)
        sock.connect(server_address)
        sock.sendall('%s %s' % (_cmd, _jstr))
        sock.settimeout(_timeout)
        _recv = sock.recv(1024*1024)
        splited = _recv.split(' ', 1)
        if splited[0] != 'ERROR':
            rc = splited[1]
        else:
            logger.error('Command server error:\n%s\n' % splited[1])
    except:
        logger.exception(traceback.format_exc()) 
    return(rc)

class IPC_Server(threading.Thread):
    ''' IPC_Server '''
    def __init__(self, _port):
        super(IPC_Server, self).__init__()
        
        self.logger = logger
        self.logger.critical('<IPC_Server> module started...')
        self.logger.critical('object up - %s\n' % str(self))
        
        self.__port = _port
        self.__cmds = {}
        self.__running = True
        self.__done_evt = threading.Event()
        self.__done_evt.clear()
        
    # _name: TEST
    # _cb: cmd_test_cb(_param: object from json.loads)
    #    return json.dumps(answer)
    def register_cmd(self, _name, _cb):
        self.__cmds[_name] = _cb
        
    def stop(self):
        self.logger.critical('IPC_Server.stop()')
        self.__running = False
        self.__done_evt.wait()
        self.logger.critical('IPC_Server stopped')
        
    def run(self):
        self.logger.info('IPC_Server.run start')
        try:
            # Create a TCP/IP socket, _port > 1023
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('localhost', self.__port)
            self.sock.bind(server_address)
            self.sock.listen(1)
            self.logger.info('Starting IPC_Server.run success')
        except:
            self.logger.exception(traceback.format_exc())
            self.logger.error('Starting IPC_Server.run failed')
            return
        
        while self.__running:
            try:
                self.sock.settimeout(1)
                connection, client_address = self.sock.accept()
            except:
                continue
            
            try:
                self.logger.info('Connection from %s' % str(client_address))
                while True:
                    self.sock.settimeout(1)
                    data = str(connection.recv(1024*1024))
                    if data:
                        try:
                            answer = ''
                            _param = None
                            _cmd = ''
                            splited = data.split(' ', 1)
                            _cmd = splited[0]
                            if _cmd in self.__cmds:
                                if len(splited) == 2:
                                    _param = splited[1]
                                answer = self.__cmds[_cmd](_param)
                            else:
                                self.logger.error('Unknown command: %s' % 
                                                  str(_cmd))
                        except:
                            _exception = traceback.format_exc()
                            self.logger.exception(_exception)
                            answer = _exception
                            _cmd = 'ERROR'
                            
                        connection.sendall('%s %s' % (_cmd, answer))
                    else:
                        self.logger.info('no more data from %s' % 
                                         str(client_address))
                        break
            finally:
                self.logger.info('Clean up the connection')
                connection.close()
                
        self.logger.info('IPC_Server.run done.')
        self.__done_evt.set()

# -----------------------------------------------------------------------------
#    end of file
# -----------------------------------------------------------------------------
