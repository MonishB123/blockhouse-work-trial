import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "mock_l1_stream"
KAFKA_SERVER = "localhost:9092"
TOTAL_ORDERS = 5000

def consume_snapshots(topic, server):
    consumer = KafkaConsumer(topic, 
                             group_id='venue_snapshot_consumer_group',
                             bootstrap_servers=server,
                             value_deseralizer= lambda v : json.loads(v.decode('utf-8')))
    unfilled_shares = TOTAL_ORDERS
    while unfilled_shares:
        message = consumer.poll()
        print(message)