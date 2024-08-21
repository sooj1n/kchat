from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        'chat',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='chat-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("채팅프로그램 - 메시지 수신")
print("메시지 대기 중 ...")

try:
    for m in consumer:
        #print(m)
        print(f"[FRIEND] {m.value['message']}")
except KeyboardInterrupt:
    print("채팅 종료")
finally:
    consumer.close()
