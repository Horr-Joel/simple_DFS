#coding:utf-8
import socket as sk,subprocess as ss
import struct
import os
sk_obj = sk.socket(sk.AF_INET,sk.SOCK_STREAM)
sk_obj.setsockopt(sk.SOL_SOCKET,sk.SO_REUSEADDR,1)  #定制重连接
sk_obj.bind(('thumm02',7777))
sk_obj.listen(5)
fileinfo_size = struct.calcsize('4s128sl')
while True:
    conn,ipaddr = sk_obj.accept()
    print("客户机ip:%s" %ipaddr[0])
    while True:
        try:

            from_recv = conn.recv(fileinfo_size)

            if from_recv:
                method, filename, filesize = struct.unpack('4s128sl', from_recv)
                filename = filename.strip('\00')
                if (method == "save"):
                    data = conn.recv(filesize)

                    # 将文件路径分割出来
                    file_dir = os.path.split(filename)[0]
                    # 判断文件路径是否存在，如果不存在，则创建，此处是创建多级目录
                    if not os.path.isdir(file_dir):
                        os.makedirs(file_dir)

                    fp = open(filename, 'wb')
                    fp.write(data)
                    fp.close()
                elif (method == "load"):
                    filesize = os.stat(filename).st_size
                    fhead = struct.pack('l',filesize)
                    conn.send(fhead)
                    fp = open(filename, 'rb')
                    data = fp.read(filesize)
                    conn.sendall(data)
                    fp.close()
            else:
                break

        except Exception:
            break
    conn.close()
sk_obj.close()

