#encoding:utf8

MT = open("MT.txt",'r')
V = open("V.txt",'r')
MTV = open("MTV.txt",'w')

MT_line = MT.readline().strip()
V_line = V.readline().strip()
MTV_lines = []

while MT_line:
    MTV_lines.append(MT_line + " " + V_line + "\n")
    MT_line = MT.readline().strip()
    V_line = V.readline().strip()
MT.close()
V.close()

#删除最后一行的换行符
MTV_lines[-1] = MTV_lines[-1][:-1]
MTV.writelines(MTV_lines)
MTV.close()
