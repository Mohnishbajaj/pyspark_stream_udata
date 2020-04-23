from pyspark import SparkContext
from pyspark.sql import SparkSession,DataFrameWriter
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cleanse_function_xfr import *
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import StructType,StringType,StructField,DateType

import json
import datetime
import time
import os
import shutil

def rename_move_file(src_direct,tgt_direct,file_name):
	for file in os.listdir(src_direct):
		if(file.endswith('.json')):		
			shutil.move(src_direct+'/'+file, tgt_direct+'/'+file_name)

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 300)

spark=SparkSession.builder.config("spark.master","local").getOrCreate()
global msg_count
msg_count=0

cleanse_country_1=udf(cleanse_country)
cln_first_last=udf(cleanse_first_last)
chk_cln_ip=udf(check_cleanse_ip)
chk_date=udf(check_date)
chk_cln_gender=udf(check_cleanse_gender)
chk_email=udf(check_email)

kvs = KafkaUtils.createDirectStream(ssc, topics=['InputUserTopic'],kafkaParams={"metadata.broker.list":"localhost:9092"})
print(kvs)
lines = kvs.map(lambda v:json.loads(v[1]))
lines.pprint()

json_file_dir='../json_raw'
country_summary_dir='../country_summary_raw'
gender_summary_dir='../gender_summary_raw'
uuser_summary_dir='../uuser_summary_raw'

json_file_tgt_dir='../json_raw_tgt'
country_summary_tgt_dir='../country_summary_tgt'
gender_summary_tgt_dir='../gender_summary_tgt'
uuser_summary_tgt_dir='../uuser_summary_tgt'

def process(rdd):
	global msg_count
	kafka_con_met_log='../log/consumer_kafka_metric'+time.strftime("%Y-%m-%d",time.localtime())+'.csv'
	log=open(kafka_con_met_log,"a+")
	log.write("Time,No_Messages\n")
	now = datetime.datetime.now()
	raw_file_tgt='raw_json'+now.strftime("%Y%m%d%H%M%S")+'.json'
	country_summary_file='country_summay_'+now.strftime("%Y%m%d%H%M%S")+'.json'
	gender_summary_file='gender_summary_'+now.strftime("%Y%m%d%H%M%S")+'.json'
	uuser_summary_file='unique_user_summary_'+now.strftime("%Y%m%d%H%M%S")+'.json'

	if(not rdd.isEmpty()):
		df=spark.createDataFrame(rdd)
		msg_count+=df.count()
		log_msg=str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))+","+str(msg_count)+"\n"
                log.write(log_msg)
                log.flush()

		df1=df.withColumn('country_txf',cleanse_country_1(df['country'])).withColumn('first_name_txf',cln_first_last(df['first_name'])).withColumn('last_name_txf',cln_first_last(df['last_name'])).withColumn('ip_address_txf',chk_cln_ip(df['ip_address'])).withColumn('date_txf',chk_date(df['date'])).withColumn('gender_txf',chk_cln_gender(df['gender'])).withColumn('email_txf',chk_email(df['email'])).withColumn('ProcessingTime',lit(now.strftime("%Y%m%d%H%M%S")))
		columns_to_drop = ['country', 'first_name','last_name','ip_address','date','gender','email']
		df1= df1.drop(*columns_to_drop)
		df1.createOrReplaceTempView('UserData')

 		countryCountDataFrame=spark.sql("select ProcessingTime, country_txf as country, count(1) as total_count from UserData group by ProcessingTime,country_txf")	
		countryCountDataFrame.show()
		genderCountDF=spark.sql("select ProcessingTime,gender_txf as gender, count(1) as gender_count from UserData group by ProcessingTime,gender_txf")	
		genderCountDF.show()
		UniqueUserCountDF=spark.sql("select ProcessingTime, 'UniqueUserCount' as Metric, count(distinct first_name_txf,last_name_txf) from UserData group by ProcessingTime")
		UniqueUserCountDF.show()

		df1.coalesce(1).write.save(path=json_file_dir, format='json', mode='append')
		countryCountDataFrame.coalesce(1).write.save(path=country_summary_dir, format='json', mode='append')
		genderCountDF.coalesce(1).write.save(path=gender_summary_dir, format='json',mode='append')	
		UniqueUserCountDF.coalesce(1).write.save(path=uuser_summary_dir,format='json',mode='append')

		rename_move_file(json_file_dir,json_file_tgt_dir,raw_file_tgt)
		rename_move_file(country_summary_dir,country_summary_tgt_dir,country_summary_file)
		rename_move_file(gender_summary_dir,gender_summary_tgt_dir,gender_summary_file)
		rename_move_file(uuser_summary_dir,uuser_summary_tgt_dir,uuser_summary_file)
		
lines.foreachRDD(process)


ssc.start()
ssc.awaitTermination()
log.close()
