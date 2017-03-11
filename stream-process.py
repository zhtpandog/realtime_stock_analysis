# - read from any kafka
# - do computation
# - write back to kafka

import sys
import json
import logging
import time
import atexit

from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logging.basicConfig()
logger = logging.getLogger('stream-process')
logger.setLevel(logging.INFO)

topic = ""
new_topic = ""
kafka_broker = ""
kafka_producer = None


def shutdown_hook(producer):
	producer.flush(10)
	producer.close(10)
	logger.info('producer closed')

def process(timeobj, rdd):
	# - calculate average
	num_of_records = rdd.count()
	if num_of_records == 0:
		logger.info('no data received')
		return
	price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda a, b: a + b)
	average = price_sum / num_of_records

	logger.info('received %d records, average price is %f' %(num_of_records, average))

	current_time = time.time()
	data = json.dumps({
		'timestamp' : current_time,
		'average' : average
		})
	try:
		kafka_producer.send(target_topic, value = data)
	except Exception as e:
		logger.warn('failed to send data to kafka')


if __name__ == '__main__':
	topic, target_topic, kafka_broker = sys.argv[1:]

	# - create spark context
	sc = SparkContext("local[2]", "StockAveragePrice") # local[2] means 2 threads
	sc.setLogLevel("INFO")
	ssc = StreamingContext(sc, 5) # 5 is time interval

	# - create a direct kafka stream for processing
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})
	kafka_producer = KafkaProducer (
		bootstrap_servers = kafka_broker
	)

	directKafkaStream.foreachRDD(process)

	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination() # can respond to keyboard termination











