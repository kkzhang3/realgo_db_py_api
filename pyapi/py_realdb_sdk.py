#
# Copyright 2024 Tai-Ji control
# This code is modified from the original version to improve the readability and maintainability of the code.
# This API is valid for the RealDB server version 1.x, 2.x, 3.x
##########################################################
import io, csv
import socket
import struct
import time
import datetime
import random
from typing import List
from enum import Enum, unique

PYREALDB_NUMPERREQ = 7200
PYREALDB_BUFFERLEN = 512

@unique
class ReturnCode(Enum):
    FAILED = 0
    OK = 1
    TIMEOUT = 100
    DATETIME_ERROR = 101
    ARGUMENT_OUT_OF_RANGE = 102
    BUSY = 200
    NULL = 201
    NOT_FOUND = 255
    CONNECT_ERROR = 300

###########################PYRealDB######################
#调用方法
# db = PYRealDB('127.0.0.1', 3001)
# endtime = datetime.datetime.now()
# startime = endtime - datetime.timedelta(minutes=1)
# status, v = db.ReadData([0,1,2], startime, endtime)
# status = db.WriteData([10, 11, 12], [1.3, 2.4, 3.5])  
#########################################################

class PYRealDB(object):

    def __init__(self, host, port, db_version=3):
        self.host = host
        self.port= port
        self.db_version = db_version

    def Ping(self, timeout=2):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
                s_client.settimeout(timeout)
                try:
                    s_client.connect((self.host, self.port))
                    ini_msg = s_client.recv(PYREALDB_BUFFERLEN)
                    ini_return, *_ = struct.unpack('4s', ini_msg[:4])
                    if ini_return != b'STAR':
                        raise Exception('connection return error')
                except Exception as e:
                    print(f"connect error: {e}")
                    return ReturnCode.CONNECT_ERROR, []

                if self.db_version == 3:
                    rndcode = random.getrandbits(64)
                    version = 0
                    format_str = '=4siQii'
                    packed_data =  struct.pack(format_str, 
                                            b'ping',
                                            16,
                                            rndcode,
                                            version,
                                            0)
                else:
                    raise Exception('unsupported server version')
                
                s_client.sendall(packed_data)
                
                return_code, return_rndcode, data = self.__recv_timeout(s_client, timeout)
                if return_code != ReturnCode.OK:
                    return return_code, []
                if return_rndcode != rndcode:
                    return ReturnCode.FAILED, []
                if self.db_version == 3:
                    format_str = '=4siQbq'
                    _vals = struct.unpack(format_str, data)
                    seconds_since_epoch = _vals[4] / 1000
                    #从msec构造时间戳
                    server_datetime = datetime.datetime.fromtimestamp(seconds_since_epoch)
                    return ReturnCode.OK, server_datetime
                else:
                    raise Exception('unsupported server version')

        except Exception as e:
            print(f"ping error: {e}")
            return ReturnCode.FAILED, []
        
    def ReadTagConfig(self, timeout=2):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
                s_client.settimeout(timeout)
                try:
                    s_client.connect((self.host, self.port))
                    ini_msg = s_client.recv(PYREALDB_BUFFERLEN)
                    ini_return, *_ = struct.unpack('4s', ini_msg[:4])
                    if ini_return != b'STAR':
                        raise Exception('connection return error')
                except Exception as e:
                    print(f"connect error: {e}")
                    return ReturnCode.CONNECT_ERROR, []

                if self.db_version == 3:
                    rndcode = random.getrandbits(64)
                    version = 0
                    format_str = '=4siQii'
                    packed_data =  struct.pack(format_str, 
                                            b'tcon',
                                            16,
                                            rndcode,
                                            version,
                                            0)
                else:
                    raise Exception('unsupported server version')
                
                s_client.sendall(packed_data)
                
                return_code, return_rndcode, data = self.__recv_timeout(s_client, timeout)
                if return_code != ReturnCode.OK:
                    return return_code, []
                if return_rndcode != rndcode:
                    return ReturnCode.FAILED, []
                if self.db_version == 3:
                    format_str = '=4siQb'
                    parsed_size = struct.calcsize(format_str)
                    csv_data = data[parsed_size:]
                    bom = b'\xef\xbb\xbf'
                    if csv_data.startswith(bom):
                        csv_data = csv_data[len(bom):]
                    csv_str = csv_data.decode('utf-8')
                    csv_file = io.StringIO(csv_str)
                    csv_reader = csv.reader(csv_file)
                    vals = []
                    for row in csv_reader:
                        vals.append(row)
                    return ReturnCode.OK, vals
                else:
                    raise Exception('unsupported server version')

        except Exception as e:
            print(f"read tag config error: {e}")
            return ReturnCode.FAILED, []

    def ReadData(self, addresses: List[int], starttime: datetime.datetime, endtime: datetime.datetime, timeout=2):  
        try:
            basetime = datetime.datetime(2017, 1, 1)
            duration = int((endtime - starttime).total_seconds())
            starttime = int((starttime - basetime).total_seconds())

            if starttime < 0 or duration < 0:
                return ReturnCode.FAILED, []

            ret_data = [[] for _ in range(len(addresses))]

            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
                s_client.settimeout(timeout)
                try:
                    s_client.connect((self.host, self.port))
                    ini_msg = s_client.recv(PYREALDB_BUFFERLEN)
                    ini_return, *_ = struct.unpack('4s', ini_msg[:4])
                    if ini_return != b'STAR':
                        raise Exception('connection return error')
                except Exception as e:
                    print(f"connect error: {e}")
                    return ReturnCode.CONNECT_ERROR, []


                for i in range(starttime, starttime + duration, PYREALDB_NUMPERREQ):
                    s_duration = min(PYREALDB_NUMPERREQ, starttime + duration - i)
                    return_code, v = self.__mk_a_read_req(s_client, addresses, i, s_duration, timeout)

                    if return_code != ReturnCode.OK:
                        return return_code, []
                    else:
                        ret_data = self.__concat_2_matrix(ret_data, v)
                        
                return return_code, ret_data

        except Exception as e:
            print(f"read DB error: {e}")
            return ReturnCode.FAILED, []


    def WriteData(self, addresses: List[int], vals: List[float], writetime: datetime.datetime = None, timeout=2):
        try:
            basetime = datetime.datetime(2017, 1, 1)
            if writetime is None:
                writetime = datetime.datetime.now()
                
            writetime = int((writetime - basetime).total_seconds())

            # 验证地址和数据的长度是否匹配
            if len(addresses) != len(vals):
                return ReturnCode.FAILED

            # 准备数据结构
            addr_vals = [val for pair in zip(addresses, vals) for val in pair]

            # 创建Socket并连接服务器
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_client:
                s_client.settimeout(timeout)
                try:
                    s_client.connect((self.host, self.port))
                    ini_msg = s_client.recv(PYREALDB_BUFFERLEN)
                    ini_return, *_ = struct.unpack('4s', ini_msg[:4])
                    if ini_return != b'STAR':
                        raise Exception('connection return error')
                except Exception as e:
                    print(f"connect error: {e}")
                    return ReturnCode.CONNECT_ERROR, []
                
                # 打包数据并发送
                if self.db_version == 3:
                    rndcode = random.getrandbits(64)
                    version = 0
                    format_str = '=4siQii'+'hf'*len(vals)
                    packed_data =  struct.pack(format_str, 
                                            b'writ',
                                            16+len(vals)*6,
                                            rndcode,
                                            version,
                                            writetime,
                                            *addr_vals)
                elif self.db_version == 2:
                    rndcode = random.getrandbits(64)
                    format_str = '=4siQi'+'hf'*len(vals)
                    packed_data =  struct.pack(format_str, 
                                            b'writ',
                                            12+len(vals)*6,
                                            rndcode,
                                            writetime,
                                            *addr_vals)
                elif self.db_version == 1:
                    rndcode = 0
                    format_str = '=4si'+'if'*len(vals)
                    packed_data =  struct.pack(format_str, 
                                            b'writ',
                                            len(vals)*8,
                                            *addr_vals)
                else:
                    raise Exception('unsupported server version')
                
                s_client.sendall(packed_data)
                
                if self.db_version == 1:
                    # 服务端版本1.x不需要等待响应
                    return_code = ReturnCode.OK
                else:
                    # 接收服务器响应，带超时处理
                    return_code, return_rndcode, data = self.__recv_timeout(s_client, timeout)
                    # 校验随机码
                    if return_code == ReturnCode.OK and return_rndcode != rndcode:
                        return_code = ReturnCode.FAILED

                return return_code

        except Exception as e:
            print(f"write DB error: {e}")
            return ReturnCode.FAILED


    def __mk_a_read_req(self, the_socket: socket.socket, _addresses, _start, _duration, timeout):
        try:
            if self.db_version == 3:
                version = 0
                rndcode = random.getrandbits(64)
                frame_len = 20+2*len(_addresses)
                format_str = '=4siQiii' + str(len(_addresses)) + 'H'
                bytes_1 = struct.pack(format_str, b'read', frame_len, rndcode, version, _start, _duration, *_addresses)
                # 打包后的数据格式为：4字节命令("read", 4s)，4字节帧长度(i)，8字节随机码(Q)，4字节版本号(i)，4字节起始时间(i)，4字节持续时间(i)，4*len字节地址数据(H)
            elif self.db_version == 2:
                rndcode = random.getrandbits(64)
                frame_len = 16+2*len(_addresses)
                format_str = '=4siQii' + str(len(_addresses)) + 'H'
                bytes_1 = struct.pack(format_str, b'read', frame_len, rndcode, _start, _duration, *_addresses)
            elif self.db_version == 1:
                rndcode = 0
                str_addresses = ','.join([str(x) for x in _addresses])
                addresses = '['+str_addresses+']'
                content = bytes(addresses+' '+str(_start)+' '+str(_duration),'ascii')
                bytes_1 = struct.pack('4si'+str(len(content))+'s', b'read',len(content), content)
            else:
                raise Exception('unsupported server version')
                
            # 发送打包后的数据
            the_socket.sendall(bytes_1)
            
            # 接收服务器响应，带超时处理
            return_code, return_rndcode, return_data = self.__recv_timeout(the_socket, timeout)

            # 服务端返回错误时直接退出
            if return_code != ReturnCode.OK:
                return return_code, []
            
            # 随机码校验
            if return_rndcode != rndcode:
                return ReturnCode.FAILED, []

            # 数据处理
            if self.db_version == 3:
                format_str = '=4siQ3B' + str(len(_addresses)*_duration) + 'f'
                _vals = struct.unpack(format_str, return_data)
                vals = _vals[6:]
                # 返回包的格式为：4字节命令，4字节帧长度，8字节随机码，1字节返回码，1字节数据校验码通道1-8，1字节数据校验码通道9-16，数据
            elif self.db_version == 2:
                format_str = '=4siQB' + str(len(_addresses)*_duration) + 'f'
                _vals = struct.unpack(format_str, return_data)
                vals = _vals[4:]
            elif self.db_version == 1:
                _vals = struct.unpack('4si'+str(len(_addresses)*_duration)+'f', return_data)
                vals = _vals[2:]
            else:
                raise Exception('unsupported server version')
            mat = self.__flat_2_matrix(vals, len(_addresses))  
            return ReturnCode.OK, mat


        except Exception as e:
            print(f"unexpect error in __mk_a_read_req: {e}")
            return ReturnCode.FAILED, []
        

    def __recv_timeout(self, the_socket: socket.socket, timeout=2):
        the_socket.settimeout(timeout)
        total_time = 0
        frame_len = 0
        total_data = b''
        total_size = 0
        return_code = ReturnCode.FAILED
        rndcode = 0
        begin_time = time.time()
        while True:
            try:
                # 接收数据
                data = the_socket.recv(PYREALDB_BUFFERLEN)
                total_size += len(data)
                total_data += data
                total_time = time.time() - begin_time
                if total_time > timeout:
                    raise TimeoutError('timeout')

                # 解码帧长度
                if len(total_data) > 8 and frame_len == 0:
                    if self.db_version in [2, 3]:
                        head, frame_len, rndcode, _return_code = struct.unpack('4siQb', total_data[:17])
                        return_code = ReturnCode(_return_code)
                    elif self.db_version == 1:
                        head, frame_len = struct.unpack('4si', total_data[:8])
                        rndcode = 0
                        return_code = ReturnCode.OK
                    else:
                        raise Exception('unsupported server version')
                    
                    if return_code != ReturnCode.OK:
                        break    # 服务端返回出错时直接退出

                # 检查是否接收完整帧
                if frame_len != 0 and frame_len+8 <= total_size:
                    break
                
            except TimeoutError as e:
                print(f"timeout error: {e}")
                return_code = ReturnCode.TIMEOUT
                break
                    
            except Exception as e:
                print(f"unexpect error at __recv_timeout: {e}")
                return_code = ReturnCode.FAILED
                break

        return return_code, rndcode, total_data


    def __flat_2_matrix(self, v1, dimension):
        len1 = len(v1) // dimension
        return [v1[i * len1:(i + 1) * len1] for i in range(dimension)]

    
    def __concat_2_matrix(self, m1, m2):
        if len(m1) != len(m2):
            return m1
        return [list(row1) + list(row2) for row1, row2 in zip(m1, m2)]

