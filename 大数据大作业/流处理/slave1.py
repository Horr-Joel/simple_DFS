#encoding:utf8
import socket
import struct
import threading
import sys
import signal

Master_Ip = "thumm01"

# 发送信息
def sendmessage(data, sign):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(10)
    sk_slave.connect((Master_Ip, 6666))
    message = struct.pack('i128s', sign, data)
    sk_slave.send(message)

    print "发送" + data + "给" + Master_Ip + "完毕"
    sk_slave.close()

bind_ip = 'thumm02'
bind_port = 6666

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((bind_ip, bind_port))
server.listen(20)  # max backlog of connections

print 'Listening on {}:{}'.format(bind_ip, bind_port)


datastream = []
fileinfo_size = struct.calcsize('i128s')

#数据处理：根据信号接收数据流或者对比数据
def handle():
    while True:
        client_sock, address = server.accept()

        from_recv = client_sock.recv(fileinfo_size)
        if from_recv:

            #sign 0 数据，1 处理请求
            sign, data  = struct.unpack('i128s', from_recv)
            data = data.strip('\00')

            if sign == 0:
                f = open("datastream2.txt",'w')
                while True:
                    data = client_sock.recv(4096)
                    if data:
                        datastream.append(data)
                        print data
                        f.write(data+"\n")
                    else:
                        client_sock.close()
            else:
                tmp  = data.split()
                timestamp_1 = tmp[2]
                value_1 = tmp[3]


                for each in datastream:

                    tmp = each.split()
                    timestamp_2 = tmp[2]
                    value_2 = tmp[3]

                    time_diff = float(timestamp_1)- float(timestamp_2)

                    #第1路数据比第2路数据时间戳小超过30秒，那么后面的没有继续比较的意义
                    if time_diff < -30:
                        break

                    if ((time_diff <= 30)  and (value_1 == value_2)):
                        data = "topic1 "+timestamp_1+" topic2 "+timestamp_2+" "+value_1
                        #结果发回Master
                        print "回传结果"
                        sendmessage(data,1)





def quit(signal_num,frame):
    print "you stop the threading"
    sys.exit()

try:
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    list = []

    t = threading.Thread(target=handle)
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=handle)
    t.setDaemon(True)
    t.start()

    while True:
        pass
except Exception,e:
    print e
