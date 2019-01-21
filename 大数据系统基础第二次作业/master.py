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
slave = ['123','thumm03']
clientIP=""


#分布式存文件   数据包序号%3： 0放01 1放12 2放02
#分布式取文件   先对每个服务器里这个文件的块排序，从2取0，1  从1取2，坏了就从3取



#发送文件
def sendfile(filelist,DestIp):
    for file in filelist:
        sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk_slave.settimeout(300)
        sk_slave.connect((DestIp, 7777))
        filesize = os.stat(file).st_size
        fhead = struct.pack('4s128sl', "save", file, filesize)
        sk_slave.send(fhead)

        fp = open(file, 'rb')
        for slice in fp:
            sk_slave.send(slice)
        fp.close()
        print "发送"+file+"给"+DestIp+"完毕"
        sk_slave.close()

#加载文件
def recvfile(filelist,DestIp):
   
    for file in filelist:
       
	sk_slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        try:
            sk_slave.connect((DestIp, 7777))
        except Exception:
            return -1

        fhead = struct.pack('4s128sl', "load", file, 0)
        sk_slave.send(fhead)

        f=open(file,'wb')
	
        while 1:
	    
            data = sk_slave.recv(1024)
	    if not data:
                break
            f.write(data)
	    
        f.close()
        print  "从 "+DestIp+"接收"+file+"完毕"
        sk_slave.close()
    return 0

#分割文件
def split(fromfile,todir,chunksize):
    if not os.path.exists(todir):
        os.mkdir(todir)
    else:
        for fname in os.listdir(todir):
            os.remove(os.path.join(todir,fname))
    partnum = 0
    inputfile = open(fromfile,'rb')
    while True:
        chunk = inputfile.read(chunksize)
        if not chunk:
            break
        partnum += 1
        filename = os.path.join(todir,('part%04d'%partnum))
        fileobj = open(filename,'wb')
        fileobj.write(chunk)
        fileobj.close()
    return partnum

#合并文件
def joinfile(fromdir,filename,todir):
    if not os.path.exists(todir):
        os.mkdir(todir)
    if not os.path.exists(fromdir):
        print('Wrong directory')
    outfile = open(os.path.join(todir,filename),'wb')
    files = os.listdir(fromdir)
    files.sort()
    for file in files:
        filepath = os.path.join(fromdir,file)
        infile = open(filepath,'rb')
        data = infile.read()
        outfile.write(data)
        infile.close()
    outfile.close()

if __name__ == '__main__':


    while True:
        conn,ipaddr = sk_client.accept()
        print("客户机ip:%s" %ipaddr[0])
        while True:

            from_recv = conn.recv(fileinfo_size)
            if from_recv:
                method,filename,filesize = struct.unpack('4s128sl', from_recv)
                filename= filename.strip('\00')
                filename = os.path.basename(filename)
                savedir = './FR/%s' % filename.split('.')[0]
                if(method=="save"):
                    filetype = filename.split('.')[1]
                    if (not os.path.exists(savedir)):
                        os.makedirs(savedir)

                    fp = open ('./FR/'+filename,'wb')

                    while  1:
                        data = conn.recv(1024)
                        if not data:
                            break
                        fp.write(data)
                    fp.close()
                    print "文件接收完毕"

                    split('./FR/'+filename,savedir,52428800)   #按照50M分块
                    print "文件分块完毕"



                    filelist0 = []
                    filelist1 = []
                    filelist2 = []
                    #0号机应删除的片
                    filelistdel = []


                    #计算文件存储信息，文件分片号从0开始
                    i = 1
                    count = 0
                    while filesize > count:
                        part_filepath = '%s/part%04d' % (savedir, i)
                        s = i % 3
                        if (s == 0):
                            filelist0.append(part_filepath)
                            filelist2.append(part_filepath)
                        elif (s == 1):
                            filelist0.append(part_filepath)
                            filelist1.append(part_filepath)
                        elif (s == 2):
                            filelist1.append(part_filepath)
                            filelist2.append(part_filepath)
                            filelistdel.append(part_filepath)
                        i = i + 1
                        count += 52428800


                    #向slave发送数据
                    sendfile(filelist1, slave[0])
                    sendfile(filelist2, slave[1])

                    print "分发完毕"

                    #01放在本机，删除s = 2
                    for file in filelistdel:
                        os.remove(file)

                    #保存文件存放信息在0号机
                    if (not os.path.exists('./record')):
                        os.mkdir('./record')

                    f = open("./record/%s.txt" % filename.split('.')[0], 'wb')
                    f.write(str(filesize))
                    f.close()

                elif(method=="load"):



                    recvlist = []
                    f = open("./record/%s.txt" % filename.split('.')[0],'rb')
                    filesize = int(f.read().strip())
                    f.close()
                    i = 0
                    count = 0
                    # 01在0号机，只需要从1号取2，1坏了则从2取2
                    while filesize > count:
                        part_filepath = '%s/part%04d' % (savedir, i)
                        s = i % 3
                        if (s == 2):
                            recvlist.append(part_filepath)
                        i = i + 1
                        count += 52428800
                    ret = recvfile(recvlist, slave[0])
                    #如果1号连接失败则从2号加载数据
                    print ret
		    if(ret==-1):
                        recvfile(recvlist, slave[1])
                    #合并分块
                    partfilepath = "./FR/"+filename.split('.')[0]
                    joinfile(partfilepath,filename,"./")
		    print "合并完毕"
                    fp = open(filename)
                    for slice in fp:
                        conn.send(slice)
                    fp.close()
                    print "发送完毕"
		    break
            else:
                break
        conn.close()
    sk_client.close()

