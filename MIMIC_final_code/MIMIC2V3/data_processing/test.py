# import time
# import datetime
# import calendar
# #import ciso8601
# 
# #print time.mktime('07-SEP-82 07.30.00.000000000 PM US/EASTERN')
# #print(time.strptime('07-SEP-82 07.30.00.000000000 PM US/EASTERN',  "%d-%m-%Y").timetuple())
# #g=time.gmtimedatetime('07-SEP-82 07.30.00.000000000 PM US/EASTERN')
# 
# 
# #print(time.mktime(g))
# 
# 
# 
# structTime = time.localtime()
# print(structTime)
# print datetime.datetime(*structTime[:6])
# #print (datetime.datetime(structTime)- datetime.datetime(1900,1,1)).total_seconds()
# print datetime.datetime(2009, 11, 8, 20, 32, 35)
# 
# 
# print (datetime.datetime(*structTime[:6]) - datetime.datetime(1900,1,1)).total_seconds()
# #1333238400.0
# #2015-09-16 01:24:54
# #2009-11-08 20:32:35
# #3651355494.0



from sklearn.preprocessing import normalize
import numpy as np
matrix = np.arange(0,27,3).reshape(3,3).astype(np.float64)

matrix  =[[  0.,   3.,   6.],[  9.,  12.,  15.],[ 18.,  21.,  24.]]

print np.max(matrix)
normed_matrix = normalize(matrix,norm='l1')
normed_matrix = matrix /  np.max(matrix)

print normed_matrix