# -*- coding: utf-8 -*-
import os
import socket
import struct
import socket as sk
import sys


reload(sys)
sys.setdefaultencoding("utf-8")

sk_client = sk.socket(sk.AF_INET,sk.SOCK_STREAM)
sk_client.setsockopt(sk.SOL_SOCKET,sk.SO_REUSEADDR,1)  #定制重连接
sk_client.bind(('szcluster.mmlab.top',6666))
sk_client.listen(5)


fileinfo_size = struct.calcsize('4s128sl')
load_size = struct.calcsize('l')
slave = ['thumm02','thumm03']
clientIP=""


#分布式存文件   数据包序号%3： 0放01 1放12 2放02
#分布式取文件   先对每个服务器里这个文件的块排序，从2取0，1  从1取2，坏了就从3取



#发送文件
def sendfile(filelist,DestIp):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(50)
    sk_slave.connect((DestIp, 7777))
    for file in filelist:
        filesize = os.stat(file).st_size
        fhead = struct.pack('4s128sl', "save", file, filesize)
        sk_slave.send(fhead)

        fp = open(file, 'rb')
        data = fp.read(filesize)
        sk_slave.sendall(data)
        fp.close()
    sk_slave.close()

#加载文件
def recvfile(filelist,DestIp):
    sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk_slave.settimeout(50)
    try:
        sk_slave.connect((DestIp, 7777))
    except sk_slave.timeout as e:
        return -1

    for file in filelist:
        fhead = struct.pack('4s128sl', "load", file, 0)
        sk_slave.send(fhead)
        fhead = sk_slave.recv(load_size)
        filesize = struct.unpack('l', fhead)

        f=open(file,'wb')
        data = sk_slave.recv(filesize)
        f.write(data)
        f.close()
    sk_slave.close()
    return 0

if __name__ == '__main__':

    DestIP="45.55.23.15"

    while True:
        conn,ipaddr = sk_client.accept()
        print("客户机ip:%s" %ipaddr[0])
        conn.send("connect succeed!")
        while True:

            from_recv = conn.recv(fileinfo_size)
            if from_recv:
                method,filename,filesize = struct.unpack('4s128sl', from_recv)
                filename= filename.strip('\00')
                filename = os.path.basename(filename)
                if(method=="save"):

                    #分片保存在本地
                    filelist0 = []
                    filelist1 = []
                    filelist2 = []
                    #0号机应删除的片
                    filelistdel = []
                    recvd_size = 0  # 定义已接收文件的大小

                    #文件分片号从0开始
                    i=0
                    savedir = './FR/%s' % filename.split('.')[0]
                    filetype = filename.split('.')[1]
                    if (not os.path.exists(savedir)):
                        os.makedirs(savedir)
                    while not recvd_size == filesize:
                        new_filename = '%s/%04d.%s' % (savedir, i, filetype)
                        if filesize - recvd_size > 1024:   #按照1K分片接收
                            data = conn.recv(1024)
                            recvd_size += len(data)

                            fp = open(new_filename, 'wb')
                            fp.write(data)
                            fp.close()
                            s = (i + 1) % 3
                            if (s == 0):
                                filelist0.append(new_filename)
                                filelist2.append(new_filename)
                            elif (s == 1):
                                filelist0.append(new_filename)
                                filelist1.append(new_filename)
                            elif (s == 2):
                                filelist1.append(new_filename)
                                filelist2.append(new_filename)
                                filelistdel.append(new_filename)
                            i = i + 1
                        else:
                            data = conn.recv(filesize - recvd_size)
                            recvd_size = filesize


                            fp = open(new_filename, 'wb')
                            fp.write(data)
                            fp.close()
                            s = (i + 1) % 3
                            if (s == 0):
                                filelist0.append(new_filename)
                                filelist2.append(new_filename)
                            elif (s == 1):
                                filelist0.append(new_filename)
                                filelist1.append(new_filename)
                            elif (s == 2):
                                filelist1.append(new_filename)
                                filelist2.append(new_filename)
                                filelistdel.append(new_filename)
                            i = i + 1

                    print "从客户端数据接收完毕"
                    #向slave发送数据
                    sendfile(filelist1, slave[0])
                    sendfile(filelist2, slave[1])
                    print "发送给从节点完毕"
                    
                    #01放在本机，删除s = 2
                    for file in filelistdel:
                        os.remove(file)

                    #保存文件存放信息在0号机
                    if (not os.path.exists('./record')):
                        os.mkdir('./record')

                    f0 = open("./record/%s0.txt" % filename.split('.')[0], 'a+')
                    for record in filelist0[:-1]:
                        f0.write("%s\n" % record)
                    f0.write("%s" % filelist0[-1])
                    f0.close()
                    f1 = open("./record/%s1.txt" % filename.split('.')[0], 'a+')
                    for record in filelist1[:-1]:
                        f1.write("%s\n" % record)
                    f1.write("%s" % filelist1[-1])
                    f1.close()
                    f2 = open("./record/%s2.txt" % filename.split('.')[0], 'a+')
                    for record in filelist2[:-1]:
                        f2.write("%s\n" % record)
                    f2.write("%s" % filelist2[-1])
                    f2.close()
                    print "保存文件存放信息完毕"
                elif(method=="load"):
                    # 01在0号机，只需要从1号取2，1坏了则从2取2
                    recvlist = []
                    f = open("./record/%s1.txt" % filename.split('.')[0],'rb')
                    for file in f.readlines():
                        if(int(file.strip('\n')[-4:])%3==2):
                            recvlist.append(file)
                    f.close()

                    ret = recvfile(recvlist, slave[0])
                    #如果1号连接失败则从2号加载数据
                    if(ret==-1):
                        recvfile(recvlist, slave[1])

                    #本机存放的分片
                    locallist = []
                    f = open("./record/%s0.txt" % filename.split('.')[0],'rb')
                    for file in f.readlines():
                        locallist.append(file.strip('\n'))
                    f.close()

                    #获得所有分片
                    locallist = locallist + recvlist
                    #去除重复元素
                    #entirelist = {}.fromkeys(locallist).keys()
                    locallist.sort()

                    # 把所有分片发送到客户端上
                    for file in locallist:
                        filesize = os.stat(file).st_size
                        fhead = struct.pack('4s128sl', "save", file, filesize)
                        conn.send(fhead)

                        fp = open(file, 'rb')
                        data = fp.read(filesize)
                        conn.sendall(data)
                        fp.close()
            else:
                break
        conn.close()
    sk_client.close()

