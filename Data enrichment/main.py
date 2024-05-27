import os
from quixstreams import Application
import redis

# Initialize Quix application
app = Application(consumer_group="transformation-v1", auto_offset_reset="earliest")

# Get input and output topics from environment variables
input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

# Create a Redis connection
r = redis.Redis(
    host=os.environ['redis_host'],
    port=int(os.environ['redis_port']),
    password=os.environ['redis_password'],
    username=os.environ['redis_username'] if 'redis_username' in os.environ else None,
    decode_responses=True)


def enrich_data(row):
    device_name = row["Device Name"]
    try:
        # Attempt to retrieve enriched data from Redis
        device_info = r.hgetall(f'sensor:{device_name}')
        if device_info:
            # Extract desired information from Redis hash (handle potential errors)
            latitude = device_info.get("lat")
            longitude = device_info.get("long")

            # Add enriched data to the row if retrieved successfully
            if latitude:
                    row["lat"] = latitude
            if longitude:
                    row["long"] = longitude
    except redis.exceptions.RedisError as e:
        print(f"Error retrieving data from Redis for {device_name}: {e}")

    return row


sdf = app.dataframe(input_topic)

# The enrich_data function will be implemented below
sdf = sdf.apply(enrich_data)

# Send enriched data to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)