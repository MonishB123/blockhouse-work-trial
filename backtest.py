import json
import time
from kafka import KafkaConsumer
import sys

KAFKA_TOPIC = "mock_l1_stream"
KAFKA_SERVER = "kafka:9092"
TOTAL_ORDERS = 5000

#functions based on allocate pseudocode
def compute_cost(split, venues, order_size, l_over, l_under, t_queue):
    executed = 0
    cash_spent = 0
    for i in range(0, len(venues)):
        exe = min(split[i], venues[i]['ask_sz_00'])
        executed += exe
        cash_spent += exe * (venues[i]['ask_px_00'] + 0) #assuming fee and rebate to be default as not providing in data
        maker_rebate = max(split[i]-exe, 0) * 0
        cash_spent -= maker_rebate
    
    underfill = max(order_size-executed, 0)
    overfill = max(executed-order_size, 0)
    risk_pen = t_queue * (underfill + overfill)
    cost_pen = l_under * underfill + l_over * overfill
    return cash_spent + risk_pen + cost_pen

def allocate(order_size, venues, l_over, l_under, t_queue):
    step = 100
    splits = [[]]
    for v in range(0, len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v]['ask_sz_00'])
            for q in range(0, max_v+1, step):
                new_splits.append(alloc+[q])
        splits = new_splits

    best_cost = sys.maxsize
    best_split = []
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, 
                            l_over, l_under, t_queue)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost

def consume_snapshots(topic, server):
    consumer = KafkaConsumer(topic, 
                             group_id='venue_snapshot_consumer_group',
                             bootstrap_servers=server,
                             value_deserializer= lambda v : json.loads(v.decode('utf-8')))
    unfilled_shares = TOTAL_ORDERS
    while unfilled_shares:
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
            for message in messages:
                print(len(message.value.get("venues")))
        

if __name__ == "__main__":
    consume_snapshots(KAFKA_TOPIC, KAFKA_SERVER)