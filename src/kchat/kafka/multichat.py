from kafka import KafkaConsumer
from kafka import KafkaProducer

import json
import time
import threading

from json import loads

p = KafkaProducer(
            bootstrap_servers=['3.38.102.55:9092'],
            #bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

c = KafkaConsumer(
        'chat',
        bootstrap_servers=['3.38.102.55:9092'],
        #bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        #enable_auto_commit=True,
        #group_id='chat-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
)

def producer(username):
    print("메시지를 입력하세요. (종료시 'exit' 입력")

    while True:
        msg = input(f"{username}: ")
        if msg.lower() == 'exit':
            break

        data = {'username': username, 'message':msg, 'time': time.time()}
        p.send('chat', value=data)

    print("채팅 종료")


def consumer():
    print("채팅프로그램 - 메시지 수신")
    print("메시지 대기 중 ...")

    try:
        for m in c:
            print(f"[{m.value['time']}] {m.value['username']} {m.value['message']}")
            #print(m)
    except KeyboardInterrupt:
        print("채팅 종료")
    finally:
        c.close()

if __name__ == "__main__":
    print("채팅 프로그램 - 메시지 발신 및 수신")
    username = input("사용할 이름을 입력하세요 : ")

    consumer_thread = threading.Thread(target=consumer)
    producer_thread = threading.Thread(target=producer, args=(username,))

    consumer_thread.start()
    producer_thread.start()

