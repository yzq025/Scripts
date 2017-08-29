################################################################################################
#
# 模块: my_load.sh
#
# 描述: 根据表名来卸载对应的数据文件 (默认以csv格式导入)
#
# 参数 1 = 表名
# 参数 2 = 排序字段名
#
################################################################################################


#========================= 参数检查 ===========================
if [ $# -lt 1 ]; then
    echo -e "\nUsage: "`basename $0`" table_name [sort column] !\n"
    echo -e "     数据库通过环境变量 MYSQL_DB_HOST MYSQL_DB_USER MYSQL_DB_PASS MYSQL_DB_NAME 来指定"
    echo -e "     文本字符集通过环境变量 MYSQL_UNLOAD_CHARSET 来指定,格式: UTF8 或 GBK"
    echo    "     文本分隔符通过环境变量 MYSQL_UNLOAD_FS 指定,例如:  \r=0x0d \n=0x0a |=0x7c ,=0x2c \t=0x09 :=0x3a #=0x23 \"=0x22 '=0x27 $=0x24"
    echo -e "     如果分隔符是0x2c或未设置环境变量，则默认按照csv格式输出\n\n"
    exit 1
else
    table_name=$1
    sor_column=$2
fi
#==============================================================



#=================== 环境变量检查 ===========================
if [ "$MYSQL_DB_USER" = "" -o "$MYSQL_DB_PASS" = "" -o "$MYSQL_DB_NAME" = "" ]; then
    echo -e "请设置MYSQL连接必要的环境变量: MYSQL_DB_USER MYSQL_DB_PASS MYSQL_DB_NAME MYSQL_DB_HOST"
    exit 2
fi
if [ "$PUB_CACHE_PATH" = "" ]; then
    PUB_CACHE_PATH="$HOME/.cache"
    [ -d $PUB_CACHE_PATH ] || mkdir -p $PUB_CACHE_PATH
fi
#===============================================================
#==============================================================



#================= 默认环境变量设置 =========================
[ "$MYSQL_DB_HOST" = "" ] && export MYSQL_DB_HOST=localhost                     #未指定默认为本机
[ "$MYSQL_UNLOAD_FS" = "" ] && export MYSQL_UNLOAD_FS=0x2c                      #未指定默认为csv格式文本
[ "$MYSQL_UNLOAD_CHARSET" = "" ] && export MYSQL_UNLOAD_CHARSET=gbk             #未指定默认为中文字符集
#==============================================================



#================== 0x2c默认为CSV格式 ==================
if [ "$MYSQL_UNLOAD_FS" = "0x2c" ]; then
    enclose_str="--fields-optionally-enclosed-by='\"'"
else
    enclose_str=""
fi
#=======================================================



#================== 执行卸载动作 ========================
echo "开始 $table_name => $table_name.txt 卸载 ..."
sqlfile=$table_name.sql
txtfile=$table_name.txt
tmpfile="$PUB_CACHE_PATH/$MYSQL_DB_NAME.$table_name.$$.sh"
rm -f $txtfile
rm -f $sqlfile
rm -f $tmpfile
echo "mysqldump -h$MYSQL_DB_HOST -u$MYSQL_DB_USER -p$MYSQL_DB_PASS --default-character-set=$MYSQL_UNLOAD_CHARSET --fields-terminated-by=$MYSQL_UNLOAD_FS --fields-escaped-by='' $enclose_str -T ./ $MYSQL_DB_NAME $table_name" >$tmpfile
result=`sh $tmpfile 2>&1`
ret=$?
rm -f $sqlfile
if [ $ret -eq 0 ]; then
    rm -f $tmpfile
    echo -e "卸载成功!!\n"
    exit 0
else
    echo "卸载失败,语句参考:"
    cat $tmpfile
    echo "结果返回..."
    echo "$result"
    rm -f $tmpfile
    exit $ret
fi
#========================================================
