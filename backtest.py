import json
import time
import numpy
from kafka import KafkaConsumer
import sys

KAFKA_TOPIC = "mock_l1_stream"
KAFKA_SERVER = "kafka:9092"
TOTAL_ORDERS = 5000

#functions based on allocate_pseudocode.txt
def compute_cost(split, venues, order_size, l_over, l_under, t_queue):
    executed = 0
    cash_spent = 0
    for i in range(0, len(venues)):
        exe = min(split[i], venues[i]['ask_sz_00'])
        executed += exe
        cash_spent += exe * (venues[i]['ask_px_00'] + 0) #assuming fee and rebate to be 0 as not providing in data
        maker_rebate = max(split[i]-exe, 0) * 0
        cash_spent -= maker_rebate
    
    underfill = max(order_size-executed, 0)
    overfill = max(executed-order_size, 0)
    risk_pen = t_queue * (underfill + overfill)
    cost_pen = l_under * underfill + l_over * overfill
    return cash_spent + risk_pen + cost_pen

#returns list of shares for each venue, 
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

#consume kafka messages until timeout reached, returns snapshots passed through producer
def consume_snapshots(topic, server):
    consumer = KafkaConsumer(topic, 
                             group_id='venue_snapshot_consumer_group',
                             bootstrap_servers=server,
                             value_deserializer= lambda v : json.loads(v.decode('utf-8')))
    
    snapshots = []
    i = 0
    while True:
        records = consumer.poll(timeout_ms=5000)

        if not records:
            print("Timeout period reached")
            break

        for tp, messages in records.items():
            for message in messages:
                snapshots.append(message.value)
    
    print(f"Finished reading batch")
    return snapshots

def run_sor(snapshots, total_order, l_over, l_under, t_queue, step=100):
    remaining = total_order
    total_cash = 0
    total_executed = 0

    for snap in snapshots:
        if remaining <= 0:
            break

        venues = snap["venues"]
        # Filter out venues with zero ask size
        venues = [v for v in venues if v['ask_sz_00'] > 0]
        if not venues:
            continue

        # Allocate shares for current snapshot
        alloc, cost = allocate(remaining, venues, l_over, l_under, t_queue)

        if not alloc or len(alloc) != len(venues):
            # Allocation failed or mismatch; skip this snapshot
            continue

        executed = sum(alloc)
        total_executed += executed
        remaining -= executed

        # Calculate cash spent this snapshot
        for i in range(len(venues)):
            fill = min(alloc[i], venues[i]['ask_sz_00'])
            total_cash += fill * venues[i]['ask_px_00']

    avg_price = total_cash / total_executed if total_executed > 0 else 0
    return {
        "total_cash": total_cash,
        "avg_fill_price": avg_price,
        "total_executed": total_executed,
        "remaining": remaining
    }


def run_baselines(snapshots, total_order):
    # Naive Best Ask baseline
    remaining = total_order
    total_cash = 0
    total_executed = 0
    for snap in snapshots:
        if remaining <= 0:
            break

        venues = sorted(snap["venues"], key=lambda v: v["ask_px_00"])
        for venue in venues:
            if remaining <= 0:
                break
            fill = min(remaining, venue["ask_sz_00"])
            total_cash += fill * venue["ask_px_00"]
            total_executed += fill
            remaining -= fill

    avg_price = total_cash / total_executed if total_executed > 0 else 0
    return {"total_cash": total_cash, "avg_fill_price": avg_price}


def parameter_search(snapshots, total_order, l_over_values, l_under_values, t_queue_values):
    best_params = None
    best_result = None
    best_cost = float('inf')

    for l_over in l_over_values:
        for l_under in l_under_values:
            for t_queue in t_queue_values:
                result = run_sor(snapshots, total_order, l_over, l_under, t_queue)
                if result["total_cash"] < best_cost:
                    best_cost = result["total_cash"]
                    best_params = {
                        "lambda_over": l_over,
                        "lambda_under": l_under,
                        "theta_queue": t_queue
                    }
                    best_result = result

    return best_params, best_result


if __name__ == "__main__":
    time.sleep(10)
    snapshots = consume_snapshots(KAFKA_TOPIC, KAFKA_SERVER)

    # Define parameter ranges to search
    l_over_values = [0.1, 0.5, 1.0]
    l_under_values = [0.1, 0.5, 1.0]
    t_queue_values = [0.1, 0.5, 1.0]

    # Run parameter search
    best_params, best_result = parameter_search(snapshots, TOTAL_ORDERS, l_over_values, l_under_values, t_queue_values)

    # Run baseline for comparison
    baseline_result = run_baselines(snapshots, TOTAL_ORDERS)

    savings_bps = 0
    if baseline_result["total_cash"] > 0:
        savings_bps = (1 - best_result["total_cash"] / baseline_result["total_cash"]) * 10000

    # Output JSON summary
    output = {
        "best_parameters": best_params,
        "optimized": {
            "total_cash": best_result["total_cash"],
            "avg_fill_px": best_result["avg_fill_price"]
        },
        "baselines": {
            "best_ask": baseline_result
        },
        "savings_vs_baselines_bps": {
            "best_ask": savings_bps
        }
    }

    print(json.dumps(output, indent=2))
