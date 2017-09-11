#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, getopt
import datetime, time
import logging
import os


try:
    import MySQLdb
    from MySQLdb import FIELD_TYPE
    from MySQLdb import converters
except:
    print
    print "    找不到MysqLdb模块,安装方法: sudo yum install MySQL-python "
    print
    sys.exit(1)


__author__ = """
yzq
"""
__copyright__ = """
Copyright 2017 yzq
"""
__version__ = "20170902"

class MysqlUtil:
    
    def __init__(self, cmdparam): 
        self.port = 3306
        self.field = "','"
        self.record = "'\\n'"
        #self.escape = "'\\\\'"
        self.escape = "''"
        self.charset = 'UTF8'
        self.inputfile = None
        self.host = '127.0.0.1'
        self.user = None
        self.passwd = None
        self.db = None
        self.table = None
        self.opt_enclose = "'\"'"
        self.logfile = None


        # 解析命令行参数  
        self.parseparam(cmdparam)
         

        #设置日志
        logging.basicConfig(filename = self.logfile, level = logging.INFO,
                format  = '%(levelname)s %(asctime)s P[%(process)d] F[%(filename)s] L[%(lineno)d]: %(message)s',
                datefmt = '%Y%m%d %H:%M:%S')
        if self.logfile <> None:
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            logging.getLogger('').addHandler(console)
        logging.info(" ".join(sys.argv))


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
                local_infile = 1,
            )
            
            cur = self.conn.cursor()
            cur.execute("SELECT VERSION()")
            data = cur.fetchone()
            logging.info("Mysql Database version : %s " % data)
#            cur.execute("set sql_mode='TRADITIONAL'")
#            logging.info("Set sql_mode='TRADITIONAL'")
            cur.close()
        except MySQLdb.Error, e:
            logging.error("code:%d msg:%s" % (e.args[0],e.args[1]))
            if cur: cur.close()
            if self.conn: self.conn.close()
            sys.exit(2)

                
    # 装载表数据  
    def loaddata(self):
        cur = self.conn.cursor()  
        sql = 'select * from ' + self.table
        sql = "load data local infile '" + self.inputfile + "' replace into table " + self.table + " character set " + self.charset + " fields terminated by " + self.field + " optionally enclosed by " + self.opt_enclose + " escaped by " + self.escape + " lines terminated by " + self.record
        logging.info("SQL: " + sql)
        try:
            cur.execute(sql)
            logging.info("Query OK: %d rows affected ..." % cur.rowcount)
            logging.info("Processed %s ..." % cur._info)
            try:
                numrows = int( cur._info.split("Records:")[1].split()[0] )
            except:
                numrows = 0
            logging.info("Total %d record imported ..." % numrows)
        except MySQLdb.Error, e:
            logging.error("code:%d msg:%s" % (e.args[0],e.args[1]))
            cur.close()
            self.conn.close()
            sys.exit(2)

        cur.close()
        self.conn.commit()
        self.conn.close()
        logging.info("Done\n")


    # 解析参数
    def parseparam(self, param):
        try:
            opts, args = getopt.getopt(param, "h:P:d:u:p:t:f:", ["field=", "record=", "charset=", "opt_enclose=", "escape=", "logfile="])
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
            elif op == "-f":
                self.inputfile = value  
            elif op == "--field":
                self.field = value   
                if value.lower()[:2] != '0x':
                    self.field = "'" + value + "'"
            elif op == "--record":
                self.record = value   
                if value.lower()[:2] != '0x':
                    self.record = "'" + value + "'"
            elif op == "--opt_enclose":
                self.opt_enclose = value   
                if value.lower()[:2] != '0x':
                    self.opt_enclose = "'" + value + "'"
            elif op == "--escape":
                self.escape = value   
                if value.lower()[:2] != '0x':
                    self.escape = "'" + value + "'"
            elif op == "--charset":
                self.charset = value
            elif op == "--logfile":
                self.logfile = value
                
        if self.host == None or self.user == None or self.password == None or self.db == None or self.table == None or self.inputfile == None:
            self.usage()
            sys.exit(1)             


    # useage
    def usage(self):
        print
        print "Usage: mysql_load.py -u root -p 123456 -d test_db -t table_name -f table.txt --charset=gbk --field=0x7c "
        print "    -u            数据库用户名 "
        print "    -p            数据库对应密码 "
        print "    -d            数据库名 "
        print "    -t            导入的表名 "
        print "    -f            导入的文件名 "
        print "    -h            主机名(可省, 默认为localhost) " 
        print "    -P            端口(可省, 默认为3306) " 
        print "    --charset     导入的文件字符集(可省, 默认为UTF8, 支持GBK) "
        print "    --field       字段分隔符(可省, 默认为 ,) "
        print "    --record      行记录分隔符(可省, 默认为 \\n), DOS格式请使用--record=\"\\r\\n\" "
        print "    --opt_enclose 字符串外包装符(可省, 默认为 \") "
        print "    --escape      转义符(可省, 默认为空不转义, 如果文本有\\N或\\n需要转义,使用--escape=\"\\\\\\\\\") "
        print "    --logfile     日志文件 "
        print
        print "    for field and record, you can use '0x' to specify hex character code,"
        print "    \\r=0x0d \\n=0x0a |=0x7c ,=0x2c, \\t=0x09, :=0x3a, #=0x23, \"=0x22 '=0x27"
        print


if __name__ == '__main__':  
    # 命令行参数初始化MysqlUtil类
    mysqlutil = MysqlUtil(sys.argv[1:])
    
    # 导入指定表数据
    mysqlutil.loaddata()
    sys.exit(0)
