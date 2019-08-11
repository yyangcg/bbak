#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/10/16 11:05
# @Author  : Yang Yuhan

from pip._vendor.pyparsing import col
from pyspark.shell import sc

sc.setLogLevel("ERROR")
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import HiveContext
import time
from pyspark.sql.functions import StringType
from pyspark.sql.functions import udf
import json


def translate(tag):
    def translate_(col):
        data = json.loads(col)
        return data.get(tag)
    return udf(translate_, StringType())


def iqiyi2tag(data_path,iqiyi_tags):
    '''
    读取iqy影片、标签
    :param data_path:
    :param iqiyi_tags:
    :return:
    '''
    iqiyi2tag_df = sc.textFile(data_path).map(lambda x: Row(iqiyi=x)).toDF()
    iqiyi2tag_df = iqiyi2tag_df.withColumn('iqytag', split(iqiyi2tag_df['iqiyi'], ',').getItem(0))
    for i in range(len(iqiyi_tags)):
        iqiyi2tag_df = iqiyi2tag_df.withColumn(iqiyi_tags[i], split(iqiyi2tag_df['iqiyi'], ',').getItem(i + 1))
    iqiyi2tag_df = iqiyi2tag_df.drop('iqiyi')
    iqy_movies = sc.textFile(data_path).map(lambda x: Row(iqiyi=x.split(",")[0])).collect()
    iqy = [item[0] for item in iqy_movies]
    return iqiyi2tag_df,iqy


def find(x,iqy):
    '''
    匹配mediadata和iqymovie
    :param x:
    :param iqy:
    :return:
    '''
    movie = ''
    for ele in iqy:
        if ele in x:
            movie = ele
            break
        else:
            continue
    return movie


def match_all(df,data_path):
    '''
    匹配所有的优酷观影记录和爱奇艺标签分数，
    存入yk2iqytag表
    :param df:
    :param data_path:
    :return:
    '''
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    # df = sqlContext.sql("select * from youku_mediadata")
    df = df.withColumn("yk_movie", translate('title')("mediadata")).filter(col('yk_movie') != '')
    iqiyi_tags = ['entertainment','technology','shopping','lifestyle','business','fashion','tourism','game','finance','female','sports','photography','car']
    iqiyi2tag_df, iqy = iqiyi2tag(data_path, iqiyi_tags)
    addcols = ['beauty', 'childcare', 'movie', 'funny', 'health', 'education', 'music', 'news']
    for i in addcols:
        iqiyi2tag_df = iqiyi2tag_df.withColumn(i, lit(0)).cache()
    yk_rdd = df.select('mediadata').rdd.map(list)
    yk_rdd = yk_rdd.map(lambda xs: [xs[0],find(xs[0],iqy)])
    df = yk_rdd.toDF(['yktag','ykmovie']).filter(col('movie')!='')
    df = df.join(iqiyi2tag_df, df.ykmovie == iqiyi2tag_df.iqytag).drop('ykmovie')
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table yk2iqytag select * from tab_name")

def create_yk2iqytag():
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    yk_mediadata = sqlContext.sql("select distinct mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt between '2018-11-01' and '2018-11-30' and channelid=3 ")
    match_all(yk_mediadata, data_path)


def youku_user_tag(data_path,month):
    '''
    优酷月表
    :param data_path:
    :param month:
    :return:
    '''
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    yk2iqytag = sqlContext.sql("select * from yk2iqytag")
    df = sqlContext.sql("select distinct idfa as userid, mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt between '2018-09-19' and '2018-09-30' and channelid=3 and length(idfa) >10")
    # df = sqlContext.sql("select distinct imeimd5 as userid, mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt between '2018-09-19' and '2018-09-30' and channelid=3 and length(imeimd5) =32")
    df = df.join(yk2iqytag, df.mediadata == yk2iqytag.yktag)
    drop_list = ['mediadata', 'yktag', 'iqytag']
    df = df.select([column for column in df.columns if column not in drop_list])
    exprs = [(avg(x)/120).alias(x) for x in df.drop('userid').columns]
    cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
    df = df.groupby("userid").agg(*exprs).withColumn('source', lit('youku')).withColumn('update_dt', lit(cur_date))
    df.registerTempTable('tab_name')
    sqlContext.sql("insert into table youku_usertag_month partition(etl_month = '" + month + "' ) select * from tab_name ")



def youku_user_tag_day(data_path,from_i=1, to_i=31):
    '''
    优酷近三十日表
    :param data_path:
    :param from_i:
    :param to_i:
    :return:
    '''
    sqlContext = HiveContext(sc)
    sqlContext.sql("use usercenter_dw")
    yk2iqytag = sqlContext.sql("select * from yk2iqytag")
    for i in range(from_i, to_i):
        date = time.strftime("%Y-%m-%d", time.localtime(int(time.time() - i * 60 * 60 * 24)))
        df = sqlContext.sql("select distinct idfa as userid, mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt = '" + date + "' and channelid=3 and length(idfa) >10")
        # df = sqlContext.sql("select distinct imeimd5 as userid, mediadata from t2pdm_data.t05_chehui_dsp_log_v2 where etl_dt = '" + date + "' and channelid=3 and length(imeimd5) =32")
        df = df.join(yk2iqytag, df.mediadata == yk2iqytag.yktag)
        drop_list = ['mediadata', 'yktag', 'iqytag']
        df = df.select([column for column in df.columns if column not in drop_list])
        exprs = [(avg(x)/120).alias(x) for x in df.drop('userid').columns]
        cur_date = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
        df = df.groupby("userid").agg(*exprs).withColumn('source', lit('youku')).withColumn('update_dt', lit(cur_date))
        df.registerTempTable('tab_name')
        sqlContext.sql("insert into table youku_usertag_day partition(etl_dt = '" + date + "' ) select * from tab_name ")
        print("finished",date)

if __name__ == '__main__':
    data_path = "hdfs://bitautodmp/user/yangyuhan1/iqy.txt"
    # match_all(data_path)
    month = '2018-11'
    youku_user_tag(data_path,month)
    youku_user_tag_day(data_path, from_i=2, to_i=31)
    #sc.parallelize([], 1).saveAsTextFile("hdfs://bitautodmp//data/bitautojob/dependences/warehouse/USERCENTER_DW/YOUKU_USER_TAG_MONTH/" +month +"/_SUCCESS")
