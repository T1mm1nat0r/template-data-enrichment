import os
from quixstreams import Application

# Create an Application.
app = Application(consumer_group="my-first-consumer-group", auto_offset_reset="earliest")

# create the input topic object and use a JSON deserializer
input_topic = app.topic(name=os.environ["input"], value_serializer="json")

sdf = app.dataframe(input_topic)

print(" ")
print(" ")
print(" ")
print("-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~")
print("Publish this data to your destination")
print("Write any Python code you need and use any Python library you fancy!")
print("-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~")
print(" ")
print(" ")
print(" ")


def publish_to_destination(row: dict):
    # write code to publish your data to any destination
    # use any Python library you like!
    print("-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~")
    print("This is one row of your data")
    print("Transform it here or publish it to an external data store")
    print(row)
    print("-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~")


sdf = sdf.apply(publish_to_destination)

if __name__ == "__main__":
    app.run(sdf)
