import kafka
import json
from iotsimulator import IotGenerator
import time
import confg as cfg


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = kafka.KafkaProducer(
    bootstrap_servers=[cfg.bootstrap],
    value_serializer=json_serializer
)

if __name__ == '__main__':
    while True:
        IoT_msg = IotGenerator()
        print(IoT_msg)
        producer.send(cfg.topic_name, IoT_msg)
        time.sleep(5)
