#
# Copyright 2018 Tai-Ji control
#
##########################################################
import socket
import struct
import time
import datetime

PYREALDB_S_OK = 1
PYREALDB_S_FAIL = -1
PYREALDB_NUMPERREQ = 300
PYREALDB_BUFFERLEN = 512
###########################PYRealDB######################
#调用方法
#startime= datetime.datetime(2018, 11, 7,13,56,00)
#endtime= datetime.datetime(2018, 11, 7,14,2,00)
#WriteData([1,2],[11.2,33.4])
#bart = PYRealDB('127.0.0.1', 3001)
#error,v=bart.ReadData([1,2,3],startime,endtime)
#########################################################
class PYRealDB(object):
	
	"""docstring for ClassName"""
	def __init__(self, host,port):
		self.host = host
		self.port= port


		
	def ReadData(self,_addresses,_starttime,_endtime):
		basetime = datetime.datetime(2017, 1, 1)
		starttime = int((_starttime-basetime).total_seconds())
		duration = int((_endtime-_starttime).total_seconds())
		if starttime<0 or duration<0:
			return [PYREALDB_S_FAIL,[]]
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
			s_client.connect((self.host, self.port))
			s_client.recv(PYREALDB_BUFFERLEN)
			ret_data=[]
			for t in range(len(_addresses)):
				ret_data.append([])
			for i in range(starttime,starttime+duration,PYREALDB_NUMPERREQ):
				s_duration = PYREALDB_NUMPERREQ
				if i+PYREALDB_NUMPERREQ > starttime+duration:
					s_duration = starttime+duration-i
				error,v=self.__mk_a_read_req(s_client,_addresses,i,s_duration)
				if error== PYREALDB_S_FAIL:
					s_client.close()
					return [PYREALDB_S_FAIL,[]]
				self.__concat_2_matrix(ret_data,v)
			s_client.close()
			
			return [PYREALDB_S_OK,ret_data]

	def WriteData(self,_addresses,_vals):
		len_addrs = len(_addresses)
		len_vals = len(_vals)
		if len_addrs!=len_vals:
			return [PYREALDB_S_FAIL]
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
			s_client.connect((self.host, self.port))
			data = s_client.recv(PYREALDB_BUFFERLEN)
			addr_vals = []
			for i in range(len_vals):
				addr_vals.append(_addresses[i])
				addr_vals.append(_vals[i])
			bytes_2 =  struct.pack('4si'+'if'*len_vals, b'writ',len_vals*8,*addr_vals)
			s_client.sendall(bytes_2)
			s_client.close()
			return [PYREALDB_S_OK]

	def __mk_a_read_req(self,the_socket,_addresses,_start,_duration):
		str_addresses = ','.join([str(x) for x in _addresses])
		addresses = '['+str_addresses+']'
		content = bytes(addresses+' '+str(_start)+' '+str(_duration),'ascii')
		bytes_1 = struct.pack('4si'+str(len(content))+'s', b'read',len(content),content)
		the_socket.sendall(bytes_1)
		timeout,data = self.__recv_timeout(the_socket)
		if timeout>0:
			return [PYREALDB_S_FAIL,[]]
		if data:
			vals= struct.unpack('4si'+str(len(_addresses)*_duration)+'f',data)
			v1 = vals[2:]
			mat=self.__flat_2_matrix(v1,len(_addresses))
			return [PYREALDB_S_OK,mat]
		else:
			return [PYREALDB_S_FAIL,[]]

	def __recv_timeout(self,the_socket,timeout=2):
		the_socket.setblocking(0)
		total_data = b''
		data = ''
		begin=time.time()
		frame_len = 0
		total_size = 0
		b_time_out = 0
		while 1:
			if total_data and time.time()-begin>timeout:
				b_time_out = 1
				break
			elif time.time()-begin>timeout*2:
				b_time_out = 1
				break
			try:
				data=the_socket.recv(PYREALDB_BUFFERLEN)
				if data:
					total_size+=len(data)
					total_data+=(data)
					begin = time.time()
					if len(total_data)>8 and frame_len == 0:
						head,frame_len=struct.unpack('4si',total_data[0:8])
					if frame_len!=0 and frame_len+8<=total_size:
						b_time_out = 0
						break
				else:
					time.sleep(0.1)
			except:
				pass
		return [b_time_out,total_data]


	def __flat_2_matrix(self,v1,dimension):
		mat = []
		len1 = int(len(v1)/dimension)
		for i in range(dimension):
			mat.append([])
			for j in range(len1):
				mat[i].append(v1[j+len1*i])
		return mat
	def __concat_2_matrix(self,m1,m2):
		if len(m1)!=len(m2):
			return m1
		for i in range(len(m1)):
			for j in range(len(m2[i])):
				m1[i].append(m2[i][j])

		return m1