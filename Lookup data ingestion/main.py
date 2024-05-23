import pandas as pd
import os
import time
import redis
from dotenv import load_dotenv
load_dotenv()

# Function to ingest data from a CSV file
def ingest_csv_data():
    csv_file_path = './sensor-data.csv'

    # Read the CSV data into a DataFrame
    df = pd.read_csv(csv_file_path)
    pipe = r.pipeline()

  # Write each row into the database
    for index, row in df.iterrows():
        # Convert row to a dictionary
        message = row.to_dict()
        message["timestamp"] = int(time.time_ns())  # Add a timestamp
        # Produce the message to the topic
        key = f'sensor:{row["name"]}'
        pipe.delete(key)
        pipe.hset(key, 'lat', row['latitude'])
        pipe.hset(key, 'long', row['longitude'])
        pipe.hset(key, 'local_time', row['local_time'])

        print(pipe.execute(), row, message)

        print(row["name"]);

    print("CSV data ingested successfully.")

r = redis.Redis(
    host=os.environ['redis_host'],
    port=int(os.environ['redis_port']),
    password=os.environ['redis_password'],
    username=os.environ['redis_username'] if 'redis_username' in os.environ else None,
    decode_responses=True)

# Run the main function
if __name__ == '__main__':
    ingest_csv_data()