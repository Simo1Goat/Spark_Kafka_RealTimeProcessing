import kafka
import json
from iotsimulator import IotGenerator
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = kafka.KafkaProducer(
    bootstrap_servers=['127.0.0.1:9091'],
    value_serializer=json_serializer
)

if __name__ == '__main__':
    while True:
        IoT_msg = IotGenerator()
        print(IoT_msg)
        producer.send("IoT_Temperature", IoT_msg)
        time.sleep(3)
