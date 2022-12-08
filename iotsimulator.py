"""
To generate <number> JSON Data:
$ ./iotsimulator.py
"""

# import the necessar libraries
import re
import datetime
import random
from random import randrange
import confg as cfg

# set the number of simulated data to generate


# let's generate the JSON Output
if __name__ == '__main__':
    num_msgs = int(input("type the number of the msgs you want to generate \n"))
    file = open("TempData.txt", "a")
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

        print(re.sub(r"[\s+]", "", cfg.iotmsg_header) % (guid, cfg.destination, state)),
        print(re.sub(r"[\s+]", "", cfg.iotmsg_eventTime) % today),
        print(re.sub(r"[\s+]", "", cfg.iotmsg_payload) % cfg.Format),
        print(re.sub(r"[\s+]", "", cfg.iotmsg_data) % cfg.current_temp[guid])
