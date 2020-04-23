from kafka import KafkaProducer
import json
import time
import os

bootstrap_servers = ['localhost:9092']
topicName = 'InputUserTopic'
json_file='../data/MOCK_DATA.json'

msg_counter=0
start_time=time.time()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


with open(json_file) as f:
	json_array=list(json.loads(f.read()))

for i in range(len(json_array)):
	time.sleep(5)
	print("sending message ",i)
	producer.send(topicName,json_array[i])
	msg_counter+=1
	if((int(time.time()-start_time))%60==0):
		print(int(time.time()-start_time))
		start_time=time.time()
		kafka_prod_met_log='../log/producer_kafka_metric'+time.strftime("%Y-%m-%d",time.localtime())+'.csv'
		log=open(kafka_prod_met_log,"a+")
		log.write("Time,No_Messages\n")
		log_msg=str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))+","+str(msg_counter)+"\n"
		log.write(log_msg)
		log.flush()
		log.close()

