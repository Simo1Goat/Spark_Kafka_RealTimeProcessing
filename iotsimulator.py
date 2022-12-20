"""
To generate <number> JSON Data:
$ ./iotsimulator.py
"""

# import the necessar libraries
import sys
import json
import re
import datetime
import random
from random import randrange
import confg as cfg
import time

# set the number of simulated data to generate
if len(sys.argv) > 1:
    num_msgs = int(sys.argv[1])
else:
    num_msgs = 1


# let's generate the JSON Output
def IotGenerator():
    file = open("data/TempData.json", "w")
    file.write("[\t\n")
    for counter in range(0, num_msgs):
        rand_num = str(randrange(0, 9)) + str(randrange(0, 9))
        rand_letter = random.choice(cfg.letters)
        temp_init_weight = random.uniform(-5, 5)
        temp_delta = random.uniform(-1, 1)

        guid = cfg.guid_base + rand_num + rand_letter
        state = random.choice(list(cfg.temp_base.keys()))

        # the first entry mapping the {guid: state}
        if guid not in cfg.device_state_map:
            cfg.device_state_map[guid] = state
            cfg.current_temp[guid] = cfg.temp_base[state] + temp_init_weight

        # the guid already exists, but it doesn't match the randomly choosen state
        elif not cfg.device_state_map[guid] == state:
            state = cfg.device_state_map[guid]

        # update the current temperature
        cfg.current_temp[guid] += temp_delta
        # getting the eventTime
        today = datetime.datetime.today().isoformat()

        data_json = re.sub(r"[\s+]", "", cfg.iot_msg) % (guid, cfg.destination, state,
                                                         today, cfg.Format, cfg.current_temp[guid])
        data_json = json.loads(data_json)
        file.write(json.dumps(data_json, indent=5))
        time.sleep(1)
        if counter == num_msgs - 1:
            continue
        else:
            file.write(",\n")

    file.write("\n]")
    file.close()


if __name__ == '__main__':
    IotGenerator()
