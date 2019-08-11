#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/04 11:07
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


def load_imei(data_path):
    return sc.textFile(data_path).map(lambda x: Row(imei_match=x)).toDF()


def load_appname2tag_df(data_path):
    return sc.textFile(data_path).map(lambda x: Row(appname=x.split("\t")[0], tag=x.split("\t")[1])).toDF()


def map_tag(app2tag):
    def mapping(col):
        tag = app2tag.get(col)
        return tag
    return udf(mapping, StringType())


def match_imei(data_path):
    '''
    --final version
    :param data_path:
    :param month:
    :return:
    '''
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    # dsp_df = sqlContext.sql("select * from t2pdm_data.t05_chehui_dsp_log_v2 where (etl_dt between '2018-09-01' and '2018-11-30') and (channelid = 4 or channelid = 5) and length(imei)>5")
    # dsp_df = sqlContext.sql("select distinct imei,mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where (etl_dt between '2018-11-01' and '2018-11-30') and channelid = 4  and length(imei)>5")
    imei_df = load_imei(data_path).persist(StorageLevel.DISK_ONLY)
    for i in range(13,31):
        date = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
        dsp_df = sqlContext.sql("select distinct imei,mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where (etl_dt = '" + date + "') and channelid = 4 and length(imei)>5")
        df = imei_df.join(dsp_df,imei_df.imei_match == dsp_df.imei)
        df.registerTempTable('tab_name')
        sqlContext.sql("insert into table yyh_imei_dsp select * from tab_name ")
        # sqlContext.sql("create table yyh_imei_dsp as select * from tab_name ")


def match(i):
    from_dt = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
    to_dt = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
    dsp_df = sqlContext.sql(
        "select distinct imei,mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where (etl_dt = '" + from_dt +  "') and channelid = 3  and length(imei)=15")
    df = imei_df.join(dsp_df, imei_df.imei_match == dsp_df.imei)
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table yyh_imei_dsp select * from tab_name ")

def match_movie():
    df = sqlContext.sql("select * from yk2iqytag inner join yyh_imei_dsp on yyh_imei_dsp.mediadata = yk2iqytag.yktag")
    drop_list = ['mediadata', 'yktag', 'iqytag','imei_match']
    df = df.select([column for column in df.columns if column not in drop_list])
    exprs = [(avg(x)/120).alias(x) for x in df.drop('imei').columns]
    cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
    df = df.groupby("imei").agg(*exprs).withColumn('source', lit('youku')).withColumn('update_dt', lit(cur_date))
    df.registerTempTable('tab_name')
    sqlContext.sql("create table yyh_imei_label as select * from tab_name ")


if __name__ == '__main__':
    data_path = "hdfs://bitautodmp/user/yangyuhan1/imei.txt"
    # match_imei(data_path)
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    imei_df = load_imei(data_path).persist(StorageLevel.DISK_ONLY)
    #match(7)
    match_movie()
    print('finished')
