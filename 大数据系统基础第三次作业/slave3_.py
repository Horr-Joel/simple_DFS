# coding:utf-8
import socket as sk
import struct
import os
import sys
# 发送文件
def sendfile(file, sign, DestIp):
    sk_slave = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
    sk_slave.settimeout(10)
    sk_slave.connect((DestIp, 6666))
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

sk_obj = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
sk_obj.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)  # 定制重连接
sk_obj.bind(('thumm04', 7777))
sk_obj.listen(5)
fileinfo_size = struct.calcsize('128si')



Master = "thumm01"
while True:
    conn, ipaddr = sk_obj.accept()
    while True:
        try:

            from_recv = conn.recv(fileinfo_size)

            if from_recv:
                #sign 11 mapper.py 12 reducer.py 13 data.txt 2 开始map 3 开始reduce
                filename, sign= struct.unpack('128si', from_recv)
                filename = filename.strip('\00')


                if(sign == 11):

                    fp = open("mapper.py", 'w')
                    while 1:
                        data = conn.recv(1024)
                        if not data:
                            break
                        fp.write(data)
                    fp.close()
                    print "接收 %s 完毕" % filename
                    break

                elif (sign == 12):

                    fp = open("reducer.py", 'w')
                    while 1:
                        data = conn.recv(1024)
                        if not data:
                            break
                        fp.write(data)
                    fp.close()
                    print "接收 %s 完毕" % filename
                    break

                elif (sign == 13):#接收data.txt

                    fp = open("data.txt", 'w')
                    while 1:
                        data = conn.recv(1024)
                        if not data:
                            break
                        fp.write(data)
                    fp.close()
                    print "接收 %s 完毕" % filename
                    break

                elif(sign == 2):#开始map
                    print "开始map"
                    os.system("chmod -x ./mapper.py")
                    os.system("cat data.txt | python mapper.py | sort -k1,1 > mapout3.txt")

                    print "map完毕，开始回传结果"
                    # 向控制节点发送map
                    sendfile("mapout3.txt", 1, Master)

                    print "回传完毕"
                    break

                elif (sign == 3):#开始reduce
                    print "开始reduce"
                    fp = open(filename, 'wb')
                    while 1:
                        data = conn.recv(1024)
                        if not data:
                            break
                        fp.write(data)
                    fp.close()
                    os.system("chmod -x ./reducer.py")
                    os.system("cat %s | python reducer.py > reduceout3.txt" % filename)

                    print "reduce完毕，开始回传结果"
                    sendfile("reduceout3.txt",2,Master)
                    #os.remove("reduceout.txt")
                    print "回传完毕"
                    break

            else:
                break

        except Exception:
            break
    conn.close()

sk_obj.close()

