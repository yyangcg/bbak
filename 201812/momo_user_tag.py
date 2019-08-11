#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/10/16 9:35
# @Author  : Yang Yuhan
from pyspark import SparkContext
sc =SparkContext()
sc.setLogLevel("ERROR")
from pyspark.sql.functions import StringType
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import HiveContext
import time
from pyspark.sql.functions import udf
import json


def translate(tag):
    def translate_(col):
        data = json.loads(col)
        return data.get(tag)
    return udf(translate_, StringType())


def momo2tag(data_path):
    return sc.textFile(data_path).map(lambda x: Row(momotag=x.split("\t")[0], tag=x.split("\t")[1])).collectAsMap()


def map_tag(momo2tag):
    def mapping(col):
        tag = momo2tag.get(col)
        return tag
    return udf(mapping, StringType())


momo_tag_list = [('10001', '游戏'),
                 ('10002', '汽车'),
                 ('10003', '旅游'),
                 ('10004', '生活'),
                 ('10005', '娱乐'),
                 ('10006', '体育'),
                 ('10007', '电影'),
                 ('10008', '时尚'),
                 ('10009', '科技'),
                 ('10010', '教育'),
                 ('10011', '教育'),
                 ('10012', '教育'),
                 ('10013', '生活'),
                 ('10014', '购物')]


def momo_user_tag(data_path,month=None,date=None):
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    df = sqlContext.sql("select userid,mediadata from yyh_momo_tmp")
    df = df.withColumn("momotag", translate('usertag')("mediadata")).select("userid", 'momotag').filter(col('momotag') != '')\
        .withColumn('momotag', explode(split('momotag', ',')))\
        .filter((length('momotag') == 5))\
        .withColumn('tag',lit(''))
    for item in momo_tag_list:
        df = df.withColumn('tag', when(col('momotag') == item[0], item[1]).otherwise(col('tag'))).cache()
    tag_list = ['娱乐', '科技', '购物', '生活', '企业办公', '时尚', '旅游', '游戏', '财经', '女性', '体育', '摄影', '汽车', '美容', '母婴育儿', '电影', '搞笑', '健康', '教育', '音乐', '资讯']
    exprs = [max(when(col("tag") == x, lit(1)).otherwise(lit(0))).alias(x) for x in tag_list]
    cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
    df = df.groupby("userid").agg(*exprs)\
        .withColumn('source', lit('momo'))\
        .withColumn('update_dt', lit(cur_date))
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table momo_usertag_month partition(etl_month = '" + month + "' ) select * from tab_name")


def get_label(date):
    df = sqlContext.sql("select userid,mediadata from yyh_momo_tmp where etl_dt = '" + date + "'")
    # df = sqlContext.sql("select distinct imeimd5 as userid, mediadata,etl_dt from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt ='"+ date + "' and channelid=4 and length(imeimd5) = 32")
    # df = sqlContext.sql("select distinct idfa as userid, mediadata,etl_dt from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt ='"+ date + "' and channelid=4 and length(idfa) > 10")
    df = df.withColumn("momotag", translate('usertag')("mediadata")).select("userid", 'momotag').filter(
        col('momotag') != '') \
        .withColumn('momotag', explode(split('momotag', ','))) \
        .filter((length('momotag') == 5)) \
        .withColumn('tag', lit(''))
    for item in momo_tag_list:
        df = df.withColumn('tag', when(col('momotag') == item[0], item[1]).otherwise(col('tag'))).cache()
    tag_list = ['娱乐', '科技', '购物', '生活', '企业办公', '时尚', '旅游', '游戏', '财经', '女性', '体育', '摄影', '汽车', '美容', '母婴育儿', '电影',
                '搞笑', '健康', '教育', '音乐', '资讯']
    exprs = [max(when(col("tag") == x, lit(1)).otherwise(lit(0))).alias(x) for x in tag_list]
    cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
    df = df.groupby("userid").agg(*exprs) \
        .withColumn('source', lit('momo')) \
        .withColumn('update_dt', lit(cur_date))
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table momo_usertag_day partition(etl_dt = '" + date + "' ) select * from tab_name ")
    print("finished", date)


def momo_day(data_path,from_i=1, to_i=31):
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    for i in range(from_i, to_i):
        date = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
        get_label(date)


if __name__ == '__main__':
    # data_path = sys.argv[1]
    data_path = "hdfs://bitautodmp/user/yangyuhan1/momo_dict.txt"
    # month,date = sys.argv[1],sys.argv[2]
    momo_user_tag(data_path,month='2018-11')
