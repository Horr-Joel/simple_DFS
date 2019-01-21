# -*- coding: utf-8 -*-
import os
import socket
import struct

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':

    print ("input: save|load file")
    method,file = raw_input().split()

    FILEINFO_SIZE = struct.calcsize('4s128sl')  # 编码格式大小

    DestIP="szcluster.mmlab.top"

    if(method=="save"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(50)
        sock.connect((DestIP, 6666))

        filesize = os.stat(file).st_size
        fhead = struct.pack('4s128sl', "save",file, filesize)  # 按照规则进行打包
        sock.send(fhead)  # 发送文件基本信息数据
        fp = open(file, 'rb')
        for slice in fp:
            sock.send(slice)
        print ("%s 发送完成..." % file)
        fp.close()

    elif(method=="load"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(300)
        sock.connect((DestIP, 6666))

        fhead = struct.pack('4s128sl', "load", file, 0)  # 按照规则进行打包
        sock.send(fhead)
        filename = os.path.basename(file)
        fp = open(filename,'wb')
        while 1:
            data = sock.recv(1024)
            if not data:
                break
            fp.write(data)
        fp.close()
        print ("%s 下载完成..." % file)
    else:
        print("错误指令")

