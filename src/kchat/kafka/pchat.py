from kafka import KafkaProducer
import time
import json

p = KafkaProducer(
        #TODO
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("채팅 프로그램 - 메시지 발신자")
print("메시지를 입력하세요. (종료시 'exit' 입력")

while True:
    msg = input("YOU: ")
    if msg.lower() == 'exit':
        break

    data = {'message':msg, 'time': time.time()}
    # TODO 보내기
    p.send('chat', value=data)

print("채팅 종료")


