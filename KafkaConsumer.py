import kafka
import json


if __name__ == '__main__':
    consumer = kafka.KafkaConsumer(
        "IoT_Temperature_",
        bootstrap_servers=['127.0.0.1:9091'],
        auto_offset_reset="earliest",
        group_id="Iot-Consumer-Group"
    )

    print("start consuming")
    for consumed in consumer:
        print(json.loads(consumed.value))
