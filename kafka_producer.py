import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
import json
import time
from datetime import datetime

CSV_PATH = "l1_day.csv"
KAFKA_TOPIC = "mock_l1_stream"
KAFKA_SERVER = "kafka:9092"
START_TIME = "2024-08-01T13:36:32"
END_TIME = "2024-08-01T13:45:14"

#Takes in path to csv file, returns a list of dictionaries containing timestamp and venue
def create_snapshots(csv_file, start_time, end_time):
    df = pd.read_csv(csv_file)
    #convert df timestamps into datetime objects
    df['timestamp'] = pd.to_datetime(df['ts_event'])
    #parse df into specified time frames
    df = df[df['timestamp'] >= pd.to_datetime(start_time).tz_localize('UTC')]
    df = df[df['timestamp'] <= pd.to_datetime(end_time).tz_localize('UTC')]
    snapshots = {}

    #populate snapshot dictionary with corresponding timestamp data for each unique timestamp
    for ts, group in df.groupby("timestamp"):
        current_time_venues = []
        for i, row in group.iterrows():
            venue_data = {
                'publisher_id' : row['publisher_id'],
                'ask_px_00' : row['ask_px_00'],
                'ask_sz_00' : row['ask_sz_00']
            }
            current_time_venues.append(venue_data)
        snapshots[ts] = current_time_venues

    # Sort timestamps to create final json data for streaming
    timestamps = sorted(snapshots.keys())
    venue_snapshots = []
    for ts in timestamps:
        venue_snapshots.append({'timestamp' : ts, 'venues' : snapshots[ts]})
    return venue_snapshots

#stream each snapshot into specified kafka topic
def stream_snapshots(snapshot_list, topic, server):
    producer = KafkaProducer(bootstrap_servers=server, 
                             value_serializer = lambda v: json.dumps(v).encode('utf-8'))

    previous_timestamp = None
    
    for snapshot_data in snapshot_list:
        #create payload of each snapshot in list
        current_timestamp = snapshot_data['timestamp']
        payload = {
            'timestamp' : current_timestamp.isoformat(),
            'venues' : snapshot_data['venues']
        }

        if previous_timestamp:
            #Calculate ts_event delta and sleep
            time_delta = (current_timestamp - previous_timestamp).total_seconds()
            time.sleep(time_delta)
        previous_timestamp = current_timestamp

        #stream data to kafka topic
        producer.send(topic, payload).get(timeout=10)
        
    if producer:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    snapshots_list = create_snapshots(CSV_PATH, START_TIME, END_TIME)
    stream_snapshots(snapshots_list, KAFKA_TOPIC, KAFKA_SERVER)