#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, getopt
import datetime, time
import binascii
try:
    import MySQLdb
    from MySQLdb import FIELD_TYPE
    from MySQLdb import converters
except:
    print
    print "    找不到MysqLdb模块,安装方法: sudo yum install MySQL-python "
    print
    sys.exit(1)


class MysqlUtil:
    
    def __init__(self, cmdparam): 
        self.port = 3306
        self.field = ','
        self.record = '\n'
        self.buffrows = 10000
        self.charset = 'UTF8'
        self.outputfile = 'mysqldump.dat'
        self.host = '127.0.0.1'
        self.user = None
        self.passwd = None
        self.db = None
        self.table = None
        self.opt_enclose = '"'

        # 解析命令行参数  
        self.parseparam(cmdparam)
         
        # 初始化连接   
        self.connectdb()
        
    
    # 检查并连接数据库
    def connectdb(self):    
        cur = None
        self.conn = None
        try:
            self.conn= MySQLdb.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                passwd = self.password,
                db = self.db,
            )
            
            self.setconversions()
            
            cur = self.conn.cursor()
            cur.execute("SELECT VERSION()")
            data = cur.fetchone()
            print "[INFO] Mysql Database version : %s " % data
            cur.execute("set names " + self.charset)
            print "[INFO] Set names : %s " % self.charset
            cur.close()
        except MySQLdb.Error, e:
            print "Error line:%d code:%d msg:%s" % (sys._getframe().f_lineno,e.args[0],e.args[1])
            if cur: cur.close()
            if self.conn: self.conn.close()
            sys.exit(2)

                
    # 设置字段解析格式
    def setconversions(self):
        conv=converters.conversions.copy()
        conv[FIELD_TYPE.DECIMAL] = float         # convert decimal to float
        conv[FIELD_TYPE.NEWDECIMAL] = float      # convert newdecimal to float
        conv[FIELD_TYPE.DATE] = str              # convert date to string
        conv[FIELD_TYPE.DATETIME] = str          # convert datetime to string
        self.conn.converter = conv   
     
    # 加入opt_enclose
    def str_conv(self,s):
        if isinstance(s,basestring) and self.opt_enclose != "":
            return self.opt_enclose + s.replace(self.opt_enclose,self.opt_enclose + self.opt_enclose) + self.opt_enclose
        else:
            return str(s)

    # 卸载表数据  
    def unloaddata(self):
        cur = self.conn.cursor()  
        sql = 'select * from ' + self.table
        try:
            cur.execute(sql)
        except MySQLdb.Error, e:
            print "Error line:%d code:%d msg:%s" % (sys._getframe().f_lineno,e.args[0],e.args[1])
            cur.close()
            self.conn.close()
            sys.exit(2)
        
        numrows = int(cur.rowcount)
        print "[INFO] Table [%s] total rows : %d" % (self.table, numrows)
        fp = open(self.outputfile, 'w')
        for i in range(numrows):
            # 一次获取 buffrows 行记录
            rows = cur.fetchmany(size=self.buffrows)
            for one_row in rows:
                line = ( "NULL" if x is None else self.str_conv(x) for x in one_row )
                fp.write(self.field.join(line) + self.record)
            
            if (i+1) % self.buffrows == 0 :
                print '[INFO] %d line exported ...' % (i+1)
        
        print '[INFO] Total %d line exported ...' % numrows 
        print '[INFO] Done ...\n' 
        fp.close()
        cur.close()
        self.conn.close()


    # 解析参数
    def parseparam(self, param):
        try:
            opts, args = getopt.getopt(param, "h:P:d:u:p:t:o:", ["field=", "record=", "buffrows=", "charset=", "opt_enclose="])
        except:
            self.usage()
            sys.exit(1)             

        for op, value in opts:
            if op == "-h":
                self.host = value
            elif op == "-P":
                self.port = int(value)
            elif op == "-d":
                self.db = value 
            elif op == "-u":
                self.user = value        
            elif op == "-p":
                self.password = value 
            elif op == "-t":
                self.table = value              
            elif op == "-o":
                self.outputfile = value  
            elif op == "--field":
                if value.lower()[:2] == '0x':
                    self.field = binascii.a2b_hex(value.replace('0x','') )
                else:
                    self.field = eval( '"' + value + '"' )
            elif op == "--record":
                if value.lower()[:2] == '0x':
                    self.record = binascii.a2b_hex(value.replace('0x','') )
                else:
                    self.record = eval( '"' + value + '"' )
            elif op == "--opt_enclose":
                if value.lower()[:2] == '0x':
                    self.opt_enclose = binascii.a2b_hex(value.replace('0x','') )
                else:
                    self.opt_enclose = eval( '"' + value + '"' )
            elif op == "--buffrows":
                self.buffrows = int(value)
            elif op == "--charset":
                self.charset = value
                
        if self.host == None or self.user == None or self.password == None or self.db == None or self.table == None:
            self.usage()
            sys.exit(1)             


    # useage
    def usage(self):
        print
        print "Usage: mysql_unload.py -u root -p 123456 -d test_db -t table_name -o dump.dat --charset=gbk --field=0x7c "
        print "    -u            数据库用户名 "
        print "    -p            数据库对应密码 "
        print "    -d            数据库名 "
        print "    -t            导出的表名 "
        print "    -o            导出的文件名(可省, 默认为当前目录mysqldump.dat) "
        print "    -h            主机名(可省, 默认为localhost) " 
        print "    -P            端口(可省, 默认为3306)" 
        print "    --charset     导出的文件字符集(可省, 默认为UTF8, 支持GBK) "
        print "    --field       字段分隔符(可省, 默认为 ,) "
        print "    --record      行记录分隔符(可省, 默认为 \\n), DOS格式请使用--record=\"\\r\\n\" "
        print "    --opt_enclose 字符串外包装符(可省, 默认为 \") "
        print "    --buffrows    一次获取记录行数(可省, 默认为 10000) "
        print
        print "    for field and record, you can use '0x' to specify hex character code,"
        print "    \\r=0x0d \\n=0x0a |=0x7c ,=0x2c, \\t=0x09, :=0x3a, #=0x23, \"=0x22 '=0x27"
        print


if __name__ == '__main__':  
    # 命令行参数初始化MysqlUtil类
    mysqlutil = MysqlUtil(sys.argv[1:])
    
    # 导出指定表数据
    mysqlutil.unloaddata()
    sys.exit(0)
