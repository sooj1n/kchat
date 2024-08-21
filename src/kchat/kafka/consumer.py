from kafka import KafkaConsumer, TopicPartition
from json import loads
import os

OFFSET_FILE = 'consumer_offset.txt'

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return None

consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000,
        #저장된게 없으면 처음부터 읽고
        auto_offset_reset='earliest' if read_offset() is None else 'none',
        #auto_offset_reset='latest' #'earliest', 'latest'
        group_id="fbi",
        enable_auto_commit=True
)

print('[Start] get consumer')
p = TopicPartition('topic1', 0)
consumer.assign([p])

if saved_offset is not None:
    consumer.seek(p,saved_offset)
else:
    consumer.seek_to_beginning(p)

for m in consumer:
    print(m)
    #print(f"topic={m.topic}, partition={m.partitions}, offset={m.offset}, value={m.value}, timestamp={msg,timestamp}")
    print(f"offset={m.offset}, value={m.value}")
    save_offset(m.offset)


print('[End] get consumer')
