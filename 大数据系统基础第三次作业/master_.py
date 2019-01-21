# -*- coding: utf-8 -*-
import os
import socket
import struct
import socket as sk
import sys
import time

reload(sys)
sys.setdefaultencoding("utf-8")


slave = ['thumm02', 'thumm03', 'thumm04']
slave_num = len(slave)


# 发送文件
def sendfile(file, sign, DestIp):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(10)
    sk_slave.connect((DestIp, 7777))
    fhead = struct.pack('128si', file, sign)
    sk_slave.send(fhead)

    if os.path.exists(file):
        fp = open(file, 'rb')
        for slice in fp:
            sk_slave.send(slice)
        fp.close()
        print "发送" + file + "给" + DestIp + "完毕"
    else:
        print "没有 %s 这个文件" % file
    sk_slave.close()

# 发送分配的reduce任务数据
def senddata(list, sign, DestIp):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(10)
    sk_slave.connect((DestIp, 7777))
    fhead = struct.pack('128si', "reduce_data.txt", sign)
    sk_slave.send(fhead)
    for line in list:
        sk_slave.send(line)
    print "发送分配的reduce数据给" + DestIp + "完毕"
    sk_slave.close()

#数据按行均分slave_num个
def splitfile(file):
    fp = open(file,'r')
    lines = fp.readlines()
    fp.close()
    chunk = int(len(lines)/slave_num)
    index = 0
    for i in range(slave_num-1):
        fp = open("data"+str(i+1)+".txt",'w')
        fp.writelines(lines[index:index+chunk])
        index += chunk
        fp.close()
    fp = open("data" + str(slave_num) + ".txt", 'w')
    fp.writelines(lines[index:])
    fp.close()


# 分配reduce任务
def shuffle():
    fp = open("mapout.txt",'r')
    lines = fp.readlines()
    lines.sort()
    length = len(lines)
    chunk = int(length/slave_num)

    index1 = chunk-10
    last = lines[index1]
    while lines[index1] == last:
        index1 += 1

    index2 = 2*chunk-10
    last = lines[index2]
    while lines[index2] == last:
        index2 += 1

    senddata(lines[:index1], 3, slave[0])
    senddata(lines[index1:index2], 3, slave[1])
    senddata(lines[index2:], 3, slave[2])




mapnum = 0
reducenum = 0

# 输入如：mapper.py reducer.py data.txt out.txt
mapfile, reducefile, inputfilename, outputfilename = raw_input("map reduce inputfilename outputfilename\n").split()

time_start = time.time()

#对数据进行分块
splitfile(inputfilename)


#分发map、reduce程序和数据
for i in range(slave_num):
    sendfile(mapfile,11,slave[i])
    sendfile(reducefile, 12, slave[i])
    sendfile("data"+str(i+1)+".txt",13,slave[i])
    sendfile("开始map信号",2,slave[i])
    #os.remove(splited_file[i])

sk_client = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
sk_client.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)  # 定制重连接
sk_client.bind(("thumm01", 6666))
sk_client.listen(10)

fileinfo_size = struct.calcsize('128si')

while True:
    conn, ipaddr = sk_client.accept()

    while True:

        from_recv = conn.recv(fileinfo_size)
        if from_recv:
            #sign 1 map结果 收到3个1代表3台机器都map完毕
            #     2 reduce结果 收到3个2代表3台机器都reduce完毕
            filename, sign = struct.unpack('128si', from_recv)
            filename = filename.strip('\00')

            if (sign == 1):
                print "接收map结果"
                fp = open(filename, 'w')
                while 1:
                    data = conn.recv(1024)
                    if not data:
                        break
                    fp.write(data)
                fp.close()
                print "接收 %s 完成" % filename
                mapnum += 1

                # 如果收到slave_num个信号代表全部map完毕
                if mapnum  == 3:

                    print "map complete"
                    fp = open("mapout.txt", 'w')
                    for filename in ("mapout1.txt", "mapout2.txt", "mapout3.txt"):
                        f = open(filename, 'r')
                        lines = f.readlines()
                        f.close()
                        fp.writelines(lines)
                        fp.write('\n')
                    fp.close()
                    #分配map结果分发给slave
                    shuffle()

            elif (sign == 2):
                print "接收reduce结果"
                fp = open(filename, 'w')
                while 1:
                    data = conn.recv(1024)
                    if not data:
                        break
                    fp.write(data)
                fp.close()
                print "接收 %s 完成" % filename
                reducenum += 1

                #如果收到slave_num个信号代表全部reduce完毕
                if reducenum == 3:
                    fp = open(outputfilename, 'w')
                    for filename in ("reduceout1.txt","reduceout2.txt","reduceout3.txt"):
                        f = open(filename,'r')
                        lines = f.readlines()
                        f.close()
                        fp.writelines(lines)
                        fp.write('\n')
                    fp.close()
                    print "reduce complete"

                    time_end = time.time()
                    print time_end - time_start,
                    print "s"
            break
        else:
            break
    conn.close()
sk_client.close()

