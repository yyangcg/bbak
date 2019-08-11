#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/10/15 11:07
# @Author  : Yang Yuhan
from pyspark import SparkContext
sc =SparkContext()
sc.setLogLevel("ERROR")
import sys
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import HiveContext
import time
from pyspark.sql.functions import StringType


def load_appname2tag(data_path):
    return sc.textFile(data_path).map(lambda x: (x.split("\t")[0], x.split("\t")[1])).collectAsMap()


def map_tag(app2tag):
    def mapping(col):
        tag = app2tag.get(col)
        return tag
    return udf(mapping, StringType())


def yichche_app(data_path,month):
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    # df = sqlContext.sql("select distinct user_id as userid, appname,etl_dt from t01_sdk_device_app_info where etl_dt between '2018-10-18' and '2018-10-31'")
    df = sqlContext.sql("select user_id as userid,appname,etl_dt from yyh_app_tmp_4")
    tag_list = ['娱乐', '科技', '购物', '生活', '企业办公', '时尚', '旅游', '游戏', '财经', '女性', '体育', '摄影', '汽车', '美容', '母婴育儿', '电影',
                '搞笑', '健康', '教育', '音乐', '资讯']
    appname2tag_dict = load_appname2tag(data_path)
    exprs = [max(when(col("tag") == x, lit(1)).otherwise(lit(0))).alias(x) for x in tag_list]
    df = df.select("userid", "appname").where(col("appname").isin(list(appname2tag_dict.keys())))
    df = df.withColumn("tag", map_tag(appname2tag_dict)('appname')).select("userid", 'tag').filter(
        (col('tag') != '') & (col('tag').isNotNull()) & (col('tag') != 'null'))
    cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
    df = df.groupby("userid").agg(*exprs)
    df = df.withColumn('source', lit('app')).withColumn('update_dt', lit(cur_date))
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table app_usertag_month partition(etl_month = '" + month + "' ) select * from tab_name ")



def yichche_app_day(data_path,from_i=1, to_i=31):
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    for i in range(from_i, to_i):
        date = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
        df = sqlContext.sql("select user_id as userid,appname,etl_dt from yyh_app_tmp_4 where etl_dt = '" + date + "'")
        tag_list = ['娱乐', '科技', '购物', '生活', '企业办公', '时尚', '旅游', '游戏', '财经', '女性', '体育', '摄影', '汽车', '美容', '母婴育儿', '电影',
                    '搞笑', '健康', '教育', '音乐', '资讯']
        appname2tag_dict = load_appname2tag(data_path)
        exprs = [max(when(col("tag") == x, lit(1)).otherwise(lit(0))).alias(x) for x in tag_list]
        df = df.select("userid", "appname").where(col("appname").isin(list(appname2tag_dict.keys()))).distinct()
        df = df.withColumn("tag", map_tag(appname2tag_dict)('appname')).select("userid", 'tag').filter(
            (col('tag') != '') & (col('tag').isNotNull()) & (col('tag') != 'null')).distinct()
        cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
        df = df.groupby("userid").agg(*exprs)
        df = df.withColumn('source', lit('app')).withColumn('update_dt', lit(cur_date))
        df.registerTempTable('tab_name')
        sqlContext.sql("insert into table app_usertag_day partition(etl_dt = '" + date + "' ) select * from tab_name ")
        print('finished',date)


if __name__ == '__main__':
    # data_path = sys.argv[1]
    data_path = "hdfs://bitautodmp/user/yangyuhan1/app_interest.txt"
    yichche_app(data_path, month='2018-11')
