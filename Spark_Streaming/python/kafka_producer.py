

import json
import argparse
import time

import pyarrow.parquet as pq
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser(description='Kafka producer')
    parser.add_argument('-d', '--dataset_path', help='Path to dataset')
    parser.add_argument('-t', '--kafka_topic', help='Kafka topic')
    parser.add_argument('-b', '--kafka_brokers', help='Kafka brokers')

    args = parser.parse_args()
    if not args.dataset_path:
        print('Please, define argument dataset_path')
        return 1
    if not args.kafka_topic:
        print('Please, define argument kafka_topic')
        return 1
    if not args.kafka_brokers:
        print('Please, define argument kafka_brokers')
        return 1

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_brokers
    )
    
    try:
        df = pq.read_table(args.dataset_path).to_pandas()
        for index, row in df.iterrows():
            data = row.to_json().encode('utf-8')
            print(data)
            producer.send(args.kafka_topic, data)
            
            time.sleep(1)
        
    except Exception as err:
        print('Kafka producer failed: %s' % str(err))
    finally:
        producer.close()
    
    
if __name__ == "__main__":
    main()