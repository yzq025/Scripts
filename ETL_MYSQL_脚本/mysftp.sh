#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import getopt
import traceback
import time
import errno

import paramiko
from paramiko.ssh_exception import BadHostKeyException
from paramiko.ssh_exception import AuthenticationException


def usage(scriptName):
    help = ("用法: %s [-r <tryTimes>] [-i <sleep interval>] hostname[:port] user passwd method<get or set> local remote\n"
            "选项介绍\n"
            "\t-r <tryTimes> 重试次数\n"
            "\t-i <sleep interval> 重试间隔时间，单位秒\n"
           )

    print help % (scriptName)


def transport(hostname, port, user, passwd, method, local, remote):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname, port=port, username=user, password=passwd)
        sftp = ssh.open_sftp()
        if method == GET_METHOD:
            sftp.get(remote, local)
        else:
            sftp.put(local, remote)

        ssh.close()
    finally:
        if ssh:
            try:
                ssh.close()
            except:
                pass


GET_METHOD = "get"
PUT_METHOD = "put"

######################################################
#                   main                             #
######################################################
if __name__ == '__main__':
    port = 22
    tryTimes = 3
    sleepInterval = 1

    # 取脚本名称
    scriptName = os.path.basename(sys.argv[0])
    opts, remainder = getopt.getopt(sys.argv[1:], "r:i:")

    for op, value in opts:
        if op == "-r":
            tryTimes = int(value)
        elif op == "-i":
            sleepInterval = int(value)

    if len(remainder) < 6:
        usage(scriptName)
        sys.exit(1)

    hostname, user, passwd, method, local, remote = remainder[0:6]

    method = method.lower()
    if method != GET_METHOD and method != PUT_METHOD:
        print "Unsupported method %s" % (method,)
        sys.exit(1)

    if hostname.find(':') >= 0:
        hostname, portstr = hostname.split(':')
        port = int(portstr)

    for i in range(tryTimes):
        if i != 0:
            time.sleep(sleepInterval)

        try:
            transport(hostname, port, user, passwd, method, local, remote)
        except (BadHostKeyException, AuthenticationException):  # 认证错误
            traceback.print_exc()
            break
        except (IOError, OSError) as e:
            traceback.print_exc()
            if e.errno in (errno.EPERM, errno.ENOENT, errno.EACCES):  # 权限、文件不存在之类的错误
                break
            continue
        except:
            traceback.print_exc()
            continue

        print "Successfully transported!"
        sys.exit(0)

    print "Error happened while transport!"
    sys.exit(1)
