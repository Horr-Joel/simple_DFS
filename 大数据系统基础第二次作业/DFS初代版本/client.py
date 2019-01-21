# -*- coding: utf-8 -*-
import os
import socket
import struct

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':

    print ("save|load file")
    method,file = raw_input().split()

    FILEINFO_SIZE = struct.calcsize('4s128sl')  # 编码格式大小

    DestIP="szcluster.mmlab.top"

    if(method=="save"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(50)
        sock.connect((DestIP, 6666))
        
        print sock.recv(1024)
        filesize = os.stat(file).st_size
        fhead = struct.pack('4s128sl', "save",file, filesize)  # 按照规则进行打包
        sock.send(fhead)  # 发送文件基本信息数据
        fp = open(file, 'rb')
        i = 1
        while 1:  # 发送文件
            filedata = fp.read(1024)
            if not filedata:
                break
            sock.sendall(filedata)
        print ("%s sending complete..." % file)
        fp.close()

    elif(method=="load"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(50)
        sock.connect((DestIP, 6666))

        fhead = struct.pack('4s128sl', "load", file, 0)  # 按照规则进行打包
        sock.send(fhead)

        #存放路径
        loaddir = './%s' % file.split('.')[0]
        if not os.path.exists(loaddir):
            os.makedirs(loaddir)
        while True:
            rec_head = sock.recv(FILEINFO_SIZE)
            if rec_head:
                method, filename, filesize = struct.unpack('4s128sl', rec_head)
                filename = filename.strip('\00')
                filename = os.path.basename(filename)
                file = os.path.basename(file)
                filename = os.path.join('%s/'% loaddir, "%s" % filename)
                data = sock.recv(filesize)
                f = open(filename,'rb')
                f.write(data)
                f.close()
            else:
                break

        #将数据分片进行合并
        filenames = os.listdir(loaddir)
        filenames.sort()

        f = open(loaddir+'/'+file, 'wb')
        # 先遍历文件名
        for filename in filenames:
            filepath = loaddir + '/' + filename
            # 遍历单个文件，读取行数
            for line in open(filepath):
                f.writelines(line)
        # 关闭文件
        f.close()
        print ("%s loading complete..." % file)
    else:
        print("wrong command")

