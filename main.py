import json,socket, logging, logging.handlers, threading, traceback, queue, sys
import zmq,time,base64, datetime, os
from PyQt5 import QtCore

log_file_format = "[%(levelname)s] - %(asctime)s - %(name)s - : %(filename)s:%(lineno)d : %(message)s"
log_console_format = "[%(levelname)s] - %(asctime)s - %(pathname)s:%(lineno)d : %(message)s"
main_logger = logging.getLogger()
main_logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_console_format))
main_logger.addHandler(console_handler)

LOG_FILE='logs/zmqout.log'
file_handler = logging.handlers.RotatingFileHandler(LOG_FILE,maxBytes = 50*1024*1024,backupCount=40)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(log_file_format))
main_logger.addHandler(file_handler)

context = zmq.Context(7)
subSocks = []
stopped=False
config={}
image_queue = queue.Queue()
saveall = False
basepath = "/home/ubuntu/work/images/archive"

class SaveFileTask(threading.Thread):
   def __init__(self, data, filename):
       # invoking the Base class
       threading.Thread.__init__(self)
       self.data = data
       self.filename = filename
       os.makedirs(filename[0:filename.rfind("/")], exist_ok=True)

   def run(self):
      # opening the file in write mode
      with open(self.filename, 'wb') as file:
         file.write(self.data)

def subscribe(command):
    logging.warning('Not support subscribe!')
    return 0

def doSend():
    global stopped
    stopped = False
    while not stopped:
        msg = image_queue.get()
        for subsock in subSocks:
            try:
                subsock.send_json(msg, zmq.DONTWAIT)
                # logging.debug('发送成功!')
            except zmq.error.Again:
                logging.warning('暂无消费端.')
    image_queue.queue.clear()
    for subsock in subSocks:
        subsock.close()

def doStart(endpoint):
    logging.info("Subscribed to {}".format(endpoint))
    global image_queue
    global context
    global subSocks
    global stopped
    global config
    global basepath
    global saveall
    pull = context.socket(zmq.PULL)
    pull.connect(endpoint)
    stopped = False
    while not stopped:
        data = pull.recv()
        # logging.debug("Received some data......")
        buf = QtCore.QByteArray.fromRawData(data)
        ds = QtCore.QDataStream(buf)
        msg_context = json.loads(ds.readQString())
        logging.debug("Received: {}".format(msg_context))

        len_data = ds.readRawData(4)
        int_len_data = int.from_bytes(len_data, "big")
        # print(int_len_data)
        data = ds.readRawData(int_len_data)
        if saveall:
            #收到图片，判断是否异步保存
            idx = msg_context['idx']
            currentTime = datetime.datetime.now()

            filepath = basepath+'/'+currentTime.strftime("%Y%m%d")
            filename= currentTime.strftime("%Y%m%d%H%M%S%f") + '-' + str(idx)+".jpg"
            fullfilename = filepath + '/' + filename

            file_write = SaveFileTask(data, fullfilename)
            # starting the task in background
            file_write.start()
        b64_string = base64.b64encode(data)
        # print(b64_string.decode('UTF-8'))
        msg = dict()
        for field in msg_context:
            msg[field] = msg_context.get(field)

        msg['img'] = b64_string.decode('UTF-8')
        image_queue.put(msg)

    pull.close()
    image_queue.queue.clear()
    saveall = False



def start(command):
    sendThred = threading.Thread(target=doSend)
    sendThred.start()

    if 'endpoints' in command:
        endpoints = command['endpoints']
        for endpoint in endpoints:
            consumeThread = threading.Thread(target = doStart,args = (endpoint,))
            consumeThread.start()
    if 'endpoint' in command:
        endpoint = command['endpoint']
        consumeThread = threading.Thread(target=doStart, args=(endpoint,))
        consumeThread.start()
    return 0

def config(command):
    global config
    global context
    global subSocks
    global saveall
    global basepath
    for subsock in subSocks:
        subsock.close()
    subSocks.clear()
    port = command['outport']
    logging.debug('Begin config output port.................')
    endpoint = "tcp://*:{}".format(port)
    push = context.socket(zmq.PUSH)
    # push.set_hwm(100)
    push.bind(endpoint)
    subSocks.append(push)
    logging.info("Config output port [{}] succeed!".format(endpoint))
    if 'saveall' in command:
        saveall = command['saveall']
    if 'basepath' in command:
        basepath = command['basepath']
    return 0


def stop(command):
    logging.info('Stopping......................')
    global stopped
    global saveall
    stopped = True
    saveall = False
    return 0


def handleCommand( clientSocket):
    while True:
        try:
            data = clientSocket.recv(1024)
            logging.debug("Received some message.")
            if not data:
                # logging.error("收到异常指令！")
                break
            cmd_str = data.decode('UTF-8')
            if len(cmd_str) == 0:
                # logging.error("收到异常指令！")
                break
            logging.debug("Received command: {}".format(cmd_str))
            command = json.loads(cmd_str)
            if command['command'] == 'start':
                result = start(command)
                # continue
            if command['command'] == 'config':
                result = config(command)
                # continue
            if command['command'] == 'subscribe':
                result = subscribe(command)
                # continue
            if command['command'] == 'stop':
                result = stop(command)
                # continue
            clientSocket.send(result.to_bytes(4,'big'))
        except ConnectionResetError as ce:
            return
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            clientSocket.close()
            return
    clientSocket.close()

if __name__ == '__main__':
    if len(sys.argv)>1:
        port = sys.argv[1]
    else:
        port = 8080
    socket = socket.socket()
    socket.bind(("", int(port)))
    socket.listen()
    logging.info("zmqout node started......")
    logging.debug("Listen on {}".format(port))
    while True:
        clientSocket,clientAddress = socket.accept()
        handleCommandThread = threading.Thread(target=handleCommand, args=(clientSocket,))
        handleCommandThread.start()

