import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import sys

KAFKA_TOPIC = "mock_l1_stream"
KAFKA_SERVER = "kafka:9092"
TOTAL_ORDERS = 5000
INTERVAL_SECONDS = 522

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
        records = consumer.poll(timeout_ms=10000)

        if not records:
            print("Timeout period reached")
            break

        for tp, messages in records.items():
            for message in messages:
                snapshots.append(message.value)
    
    print(f"Finished reading batch")
    return snapshots

def run_sor(snapshots, total_order, l_over, l_under, t_queue):
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

#Naive Best Ask baseline test
def run_naive(snapshots):
    remaining = TOTAL_ORDERS
    total_cash = 0
    total_executed = 0
    for snap in snapshots:
        if remaining <= 0:
            break
        #fill max shares for lowest price in each snapshot
        venue = min(snap["venues"], key=lambda v: v["ask_px_00"])
        fill = min(remaining, venue["ask_sz_00"])
        total_cash += fill * venue["ask_px_00"]
        total_executed += fill
        remaining -= fill

    avg_price = total_cash / total_executed if total_executed > 0 else 0
    return {"total_cash": total_cash, "avg_fill_price": avg_price}

#TWAP baseline test
def run_twap(snapshots):
    remaining = TOTAL_ORDERS
    total_cash = 0
    total_executed = 0
    start_time = pd.to_datetime(snapshots[0]["timestamp"], utc=True)
    max_fill = TOTAL_ORDERS // (INTERVAL_SECONDS // 60)
    for snap in snapshots[1:len(snapshots)-1]:
        if remaining <= 0:
            break
        #if duration from last action is > 60, buy order size split evenly from total trading duration with prioritized lowest ask px
        if (pd.to_datetime(snap["timestamp"], utc=True) - start_time).seconds >= 60 or max_fill != (TOTAL_ORDERS // (INTERVAL_SECONDS // 60)):
            start_time = pd.to_datetime(snap["timestamp"], utc=True)
            venues = sorted(snap["venues"], key= lambda v: v["ask_px_00"])
            cheapest_venue_i = 0
            #fill cheapest ask prices completely until split size is reached
            while max_fill > 0:
                if cheapest_venue_i >= len(venues):
                    break
                fill = min(max_fill, venues[cheapest_venue_i]["ask_sz_00"])
                total_cash += fill * venues[cheapest_venue_i]["ask_px_00"]
                total_executed += fill
                remaining -= fill
                max_fill -= fill 
                cheapest_venue_i += 1
            
            if max_fill <= 0:
                max_fill = TOTAL_ORDERS // (INTERVAL_SECONDS // 60)
    
    #if split is uneven, buy all possible shares during last snapshot
    print(total_executed, remaining, sep=" ")
    if total_executed < remaining:
        venues = sorted(snapshots[-1]["venues"], key= lambda v: v["ask_px_00"])
        cheapest_venue_i = 0
        while remaining > 0:
            if cheapest_venue_i >= len(venues):
                break
            fill = min(remaining, venues[cheapest_venue_i]["ask_sz_00"])
            total_cash += fill * venues[cheapest_venue_i]["ask_px_00"]
            total_executed += fill
            remaining -= fill
            cheapest_venue_i += 1

    avg_price = total_cash / total_executed if total_executed > 0 else 0
    return {"total_cash": total_cash, "avg_fill_price": avg_price}

#VWAP baseline test
def run_vwap(snapshots):
    total_cash = 0
    total_executed = 0
    remaining = TOTAL_ORDERS

    # Compute total visible liquidity across all snapshots
    snapshot_liquidities = []
    total_liquidity = 0
    for snap in snapshots:
        liquidity = sum([v["ask_sz_00"] for v in snap["venues"]])
        snapshot_liquidities.append(liquidity)
        total_liquidity += liquidity

    if total_liquidity == 0:
        return {"total_cash": 0, "avg_fill_price": 0, "total_executed": 0}

    # Proportionally allocate order
    for i, snap in enumerate(snapshots):
        if remaining <= 0:
            break

        snap_liquidity = snapshot_liquidities[i]
        if snap_liquidity == 0:
            continue

        # Proportion of total liquidity this snapshot contributes
        share_fraction = snap_liquidity / total_liquidity
        shares_to_fill = min(remaining, int(round(TOTAL_ORDERS * share_fraction)))

        venues = sorted(snap["venues"], key=lambda v: v["ask_px_00"])
        for venue in venues:
            if shares_to_fill <= 0:
                break
            fill = min(shares_to_fill, venue["ask_sz_00"])
            total_cash += fill * venue["ask_px_00"]
            total_executed += fill
            shares_to_fill -= fill
            remaining -= fill
    
    #repeat if order limit is not reached
    if remaining > 0:
        for snap in snapshots:
            venues = sorted(snap["venues"], key=lambda v: v["ask_px_00"])
            for venue in venues:
                if remaining <= 0:
                    break
                fill = min(remaining, venue["ask_sz_00"])
                total_cash += fill * venue["ask_px_00"]
                total_executed += fill
                remaining -= fill

            if remaining <= 0:
                break
    avg_price = total_cash / total_executed if total_executed > 0 else 0
    return {
        "total_cash": total_cash,
        "avg_fill_price": avg_price
    }
            
                
# Simple Gaussian Kernel
def rbf_kernel(X1, X2, length_scale=1.0, sigma_f=1.0):
    sqdist = np.sum((X1[:, np.newaxis] - X2[np.newaxis, :]) ** 2, axis=2)
    return sigma_f ** 2 * np.exp(-0.5 / length_scale ** 2 * sqdist)

# Predict mean and variance
def gp_predict(X_train, Y_train, X_test, length_scale=1.0, sigma_f=1.0, noise=1e-6):
    K = rbf_kernel(X_train, X_train, length_scale, sigma_f) + noise * np.eye(len(X_train))
    K_s = rbf_kernel(X_train, X_test, length_scale, sigma_f)
    K_ss = rbf_kernel(X_test, X_test, length_scale, sigma_f) + noise * np.eye(len(X_test))
    K_inv = np.linalg.inv(K)

    mu_s = K_s.T.dot(K_inv).dot(Y_train)
    cov_s = K_ss - K_s.T.dot(K_inv).dot(K_s)
    return mu_s, np.sqrt(np.diag(cov_s))

# Expected Improvement (EI)
def expected_improvement(X, X_sample, Y_sample, length_scale=1.0, sigma_f=1.0, noise=1e-6, xi=0.01):
    mu, sigma = gp_predict(X_sample, Y_sample, X, length_scale, sigma_f, noise)
    mu_sample_opt = np.min(Y_sample)

    with np.errstate(divide='warn'):
        imp = mu_sample_opt - mu - xi
        Z = imp / sigma
        ei = imp * norm_cdf(Z) + sigma * norm_pdf(Z)
        ei[sigma == 0.0] = 0.0
    return ei

# Normal PDF and CDF
def norm_pdf(x): 
    return (1.0 / np.sqrt(2 * np.pi)) * np.exp(-0.5 * x**2)
def norm_cdf(x): 
    return (1.0 + erf(x / np.sqrt(2))) / 2

#approximation of error function using Abramowitz and Stegun formula
def erf(x):  
    sign = np.sign(x)
    x = np.abs(x)
    t = 1.0 / (1.0 + 0.3275911 * x)
    y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t
                - 0.284496736) * t + 0.254829592) * t * np.exp(-x * x)
    return sign * y

# Random search for next best candidate based on EI
def propose_location(X_sample, Y_sample, bounds, n_candidates=1000):
    X_random = np.random.uniform(bounds[:, 0], bounds[:, 1], size=(n_candidates, bounds.shape[0]))
    ei = expected_improvement(X_random, X_sample, Y_sample)
    return X_random[np.argmax(ei)]

# Main optimization loop
def bayesian_optimization(run_sor_fn, snapshots, total_order, n_iter=20, init_points=5):
    bounds = np.array([[0.01, 0.3], [0.01, 0.3], [0.01, 0.3]])  # over, under, queue
    X_sample = np.random.uniform(bounds[:, 0], bounds[:, 1], size=(init_points, bounds.shape[0]))
    Y_sample = []
    executed_sample = []

    for x in X_sample:
        res = run_sor_fn(snapshots, total_order, x[0], x[1], x[2])
        Y_sample.append(res["total_cash"])
        executed_sample.append(res.get("total_executed", 0))
    Y_sample = np.array(Y_sample)
    executed_sample = np.array(executed_sample)

    for i in range(n_iter):
        x_next = propose_location(X_sample, Y_sample, bounds)
        res = run_sor_fn(snapshots, total_order, x_next[0], x_next[1], x_next[2])
        y_next = res["total_cash"]

        X_sample = np.vstack((X_sample, x_next))
        Y_sample = np.append(Y_sample, y_next)
        executed_sample = np.append(executed_sample, res.get("total_executed", 0))

    best_idx = np.argmin(Y_sample)
    return {
        "best_parameters": {
            "lambda_over": X_sample[best_idx][0],
            "lambda_under": X_sample[best_idx][1],
            "theta_queue": X_sample[best_idx][2],
        },
        "total_cash": Y_sample[best_idx],
        "total_executed": executed_sample[best_idx]
    }


if __name__ == "__main__":
    time.sleep(10)
    snapshots = consume_snapshots(KAFKA_TOPIC, KAFKA_SERVER)

    # Run parameter search
    print("starting bayesian optimization")
    parameter_results = bayesian_optimization(run_sor, snapshots, TOTAL_ORDERS)

    # Run baseline for comparison
    print("running baseline tests")
    best_ask_baseline_result = run_naive(snapshots)
    best_ask_savings_bps = 0
    if best_ask_baseline_result["total_cash"] > 0:
        best_ask_savings_bps = (1 - parameter_results["total_cash"] / best_ask_baseline_result["total_cash"]) * 10000

    twap_baseline_result = run_twap(snapshots)
    twap_savings_bps = 0
    if twap_baseline_result["total_cash"] > 0:
        twap_savings_bps = (1 - parameter_results["total_cash"] / twap_baseline_result["total_cash"]) * 10000

    vwap_baseline_result = run_vwap(snapshots)
    vwap_savings_bps = 0
    if vwap_baseline_result["total_cash"] > 0:
        vwap_savings_bps = (1 - parameter_results["total_cash"] / vwap_baseline_result["total_cash"]) * 10000

    # Output JSON summary
    output = {
        "best_parameters": parameter_results["best_parameters"],
        "optimized": {
            "total_cash": parameter_results["total_cash"],
            "avg_fill_px": parameter_results["total_cash"] / parameter_results["total_executed"]
        },
        "baselines": {
            "best_ask": best_ask_baseline_result,
            "twap" : twap_baseline_result,
            "vwap" : vwap_baseline_result
        },
        "savings_vs_baselines_bps": {
            "best_ask": best_ask_savings_bps,
            "twap" : twap_savings_bps,
            "vwap" : vwap_savings_bps
        }
    }

    print(json.dumps(output, indent=2))
