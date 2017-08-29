################################################################################################
#
# 模块: my_load.sh
#
# 描述: 根据表名来装载对应的数据文件 (默认以csv格式导入)
#
# 参数 1 = 文件名
# 参数 2 = 表名
#
################################################################################################


#========================= 参数检查 ===========================
if [ $# -lt 2 ]; then
    echo -e "\nUsage: "`basename $0`" file_name table_name!\n"
    echo -e "     数据库通过环境变量 MYSQL_DB_HOST MYSQL_DB_USER MYSQL_DB_PASS MYSQL_DB_NAME 来指定"
    echo -e "     文本字符集通过环境变量 MYSQL_LOAD_CHARSET 来指定,格式: UTF8 或 GBK"
    echo    "     文本分隔符通过环境变量 MYSQL_LOAD_FS 指定,例如:  \r=0x0d \n=0x0a |=0x7c ,=0x2c \t=0x09 :=0x3a #=0x23 \"=0x22 '=0x27 $=0x24"
    echo -e "     如果分隔符是0x2c或未设置环境变量，则默认按照csv格式输出\n\n"
    exit 1
else
    file_name=$1
    table_name=$2
fi
#==============================================================



#=================== 环境变量检查 ===========================
if [ "$MYSQL_DB_USER" = "" -o "$MYSQL_DB_PASS" = "" -o "$MYSQL_DB_NAME" = "" ]; then
    echo -e "请设置MYSQL连接必要的环境变量: MYSQL_DB_USER MYSQL_DB_PASS MYSQL_DB_NAME MYSQL_DB_HOST"
    exit 2
fi
#==============================================================



#================= 默认环境变量设置 =========================
[ "$MYSQL_DB_HOST" = "" ] && export MYSQL_DB_HOST=localhost                     #未指定默认为本机
[ "$MYSQL_LOAD_FS" = "" ] && export MYSQL_LOAD_FS=0x2c                          #未指定默认为csv格式文本
[ "$MYSQL_LOAD_CHARSET" = "" ] && export MYSQL_LOAD_CHARSET=gbk                 #未指定默认为中文字符集
#==============================================================



#================ 检查数据库连接串 ======================
mysql_flag="-h$MYSQL_DB_HOST -u$MYSQL_DB_USER -p$MYSQL_DB_PASS -D$MYSQL_DB_NAME"
echo -e "quit\n"|mysql $mysql_flag > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "数据库连接失败[$mysql_flag],请检查相应环境变量"
    exit 3
fi
#========================================================



#=================== 检查文件名 =========================
if [ ! -f $file_name ]; then
    echo -e "参数错 => 文本[$file_name]不存在!"
    exit 4
fi
#========================================================




#================== 生成控制脚本 ========================
if [ "$MYSQL_LOAD_FS" = "0x2c" ]; then
    enclose_str="optionally enclosed by '\"' escaped by ''"
else
    enclose_str=""
fi
file $file_name|grep -q "with CRLF line terminators"
if [ $? -eq 0 ]; then
    lines_terminated_str="lines terminated by '\r\n'"
else
    lines_terminated_str=""
fi
cmd="mysql $mysql_flag -N"
sql="load data local infile '$file_name' into table $table_name character set $MYSQL_LOAD_CHARSET fields terminated by $MYSQL_LOAD_FS $enclose_str $lines_terminated_str"
#========================================================




#================== 执行装载动作 ========================
echo "开始 $file_name => $table_name 装载 ..."
result=`$cmd 2>&1 <<!
set sql_mode='TRADITIONAL';
$sql
!`
ret=$?
if [ $ret -eq 0 ]; then
    echo -e "装载成功!!\n"
    exit 0
else
    echo "装载失败,语句参考:"
    echo "$cmd -e \"$sql\""
    echo "结果返回..."
    echo "$result"
    exit $ret
fi
#========================================================
