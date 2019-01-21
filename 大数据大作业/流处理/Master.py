#encoding:utf8


#encoding:utf8
import socket
import random
import time
import threading
import struct
import sys
import signal

datastream = []

slave1_IP='thumm02'

slave2_IP='thumm03'

slave3_IP='thumm04'

offset_1 = 0#random.randint(0, 86400)   # initial time difference (less than 1 day)
offset_2 = 33#random.randint(0, 86400)   # initial time difference (less than 1 day)

#第一路数据流
def senddata():



    random.seed(time.time())


    speed_level = random.randint(1, 4)   # stream speed level

    topic = 'topic ' + str(1) + ' '
    f = open("datastream1.txt",'w')
    while True:
        t = time.time() + offset_1
        x = random.randint(0, 2**10)    # int_val range [0,1024]

        data = topic + str(t) + " " + str(x)
        print data
        f.write(data+"\n")
        datastream.append(data)

        time.sleep(max(0.1, random.randint(0, speed_level * 400) / 1000.0))    # 0.1 <= stream interval <= level * 0.4 (seconds)

#分发第二路数据流
def sendmutildata():

    client1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client1.connect((slave1_IP, 6666))

    client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client2.connect((slave2_IP, 6666))

    client3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client3.connect((slave3_IP, 6666))

    random.seed(time.time())

    speed_level = random.randint(1, 4)   # stream speed level

    topic = 'topic ' + str(2) + ' '

    fhead = struct.pack('i128s', 0, "xxx")
    client1.send(fhead)
    client2.send(fhead)
    client3.send(fhead)
    while True:
        t = time.time() + offset_2
        x = random.randint(0, 2**10)    # int_val range [0,1024]
        data = topic + str(t) + " " + str(x)
        client1.send(data)
        client2.send(data)
        client3.send(data)
        time.sleep(max(0.1, random.randint(0, speed_level * 400) / 1000.0))    # 0.1 <= stream interval <= level * 0.4 (seconds)

# 发送数据函数
def sendmessage(IP, data):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(10)
    sk_slave.connect((IP, 6666))
    message = struct.pack('i128s', 1, data)
    sk_slave.send(message)

    sk_slave.close()


#同步等待后分配数据处理任务
def assignment():
    i = 0

    while True:
        try:
            # 从第一路数据流中取出一条数据
            tmp = datastream[i]
            # 根据差来判断是否发送或等待
            diff = float(tmp.split()[2]) - time.time() - offset_2 + 30
            if(diff <= 0):
                time.sleep(0.1)
                if i%3 == 0:
                    sendmessage(slave1_IP, tmp)
                    i += 1
                elif i%3 == 1:
                    sendmessage(slave2_IP, tmp)
                    i += 1
                else:
                    sendmessage(slave3_IP, tmp)
                    i += 1
            else:
                time.sleep(diff)
        except IndexError:
            pass

#接收处理结果
def recvout():
    bind_ip = 'thumm01'
    bind_port = 6666

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((bind_ip, bind_port))
    server.listen(20)  # max backlog of connections

    fileinfo_size = struct.calcsize('i128s')

    fn = open("result.txt", 'w')
    while True:
        client_sock, address = server.accept()
        #print 'Accepted connection from {}:{}'.format(address[0], address[1])
        from_recv = client_sock.recv(fileinfo_size)
        if from_recv:

            sign, data  = struct.unpack('i128s', from_recv)
            data = data.strip('\00')
            print data
            fn.write(data+"\n")
        client_sock.close()




def quit(signal_num,frame):
    print "you stop the threading"
    sys.exit()

try:
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    list = []

    t = threading.Thread(target=senddata)
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=sendmutildata)
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=assignment)
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=recvout)
    t.setDaemon(True)
    t.start()

    while True:
        pass
except Exception,e:
    print e
