"""
 this file contains:
    * the average annual temperature of each state
    * some fixed values
"""

from pyspark.sql.types import *

# mapping of guid and a state {guid: state}
device_state_map = {}

# the average annual temperature of each state
temp_base = {
    'WA': 48.3, 'DE': 55.3, 'DC': 58.5, 'WI': 43.1, 'WV': 51.8, 'HI': 70.0, 'FL': 70.7,
    'WY': 42.0, 'NH': 43.8, 'NJ': 52.7, 'NM': 53.4, 'TX': 64.8, 'LA': 66.4, 'NC': 59.0,
    'NE': 48.8, 'TN': 57.6, 'NY': 45.4, 'PA': 48.8, 'CA': 59.4, 'NV': 49.9, 'VA': 55.1,
    'CO': 45.1, 'AK': 26.6, 'AL': 62.8, 'AR': 60.4, 'VT': 42.9, 'IL': 51.8, 'GA': 63.5,
    'IN': 51.7, 'IA': 47.8, 'OK': 59.6, 'AZ': 60.3, 'ID': 44.4, 'CT': 49.0, 'ME': 41.0,
    'MD': 54.2, 'MA': 47.9, 'OH': 50.7, 'UT': 48.6, 'MO': 54.5, 'MN': 41.2, 'MI': 44.4,
    'RI': 50.1, 'KS': 54.3, 'MT': 42.7, 'MS': 63.4, 'SC': 62.4, 'KY': 55.6, 'OR': 48.4,
    'SD': 45.2
}

# latest temperature measered by sensors {guid: temperature}
current_temp = {}

# Fixed values
guid_base = "0-ZZZ12345678-"
destination = "0-AAA12345678"
Format = "urn:example:sensor:temp"

# choice of random letter
letters = "ABCDEFGHIJKLMONPQRSTUVWXYZ"

iot_msg = """\
{
     "guid": "%s",
     "destination": "%s",
     "state": "%s",
     "evenTime": "%sZ",
     "payload": {
        "format": "%s",
        "data":{ 
            "temperature": "%.1f"
        }
    }
}
"""

# PySpark Processing Phase
topic_name = "IoT_Temperature_"
bootstrap = "127.0.0.1:9091"
auto_reset_offset = "earliest"


mySchema = StructType([
    StructField("guid", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("state", StringType(), True),
    StructField("eventTime", TimestampType(), True),
    StructField("payload", StructType([
        StructField("format", StringType(), True),
        StructField("data", StructType([
            StructField("temperature", StringType(), True)
        ]))
    ]))
])


BASE_PATH = "C:\\Users\\pc\\Desktop\\iid3\\big_data\\IOT_Project\\data"
WRITE_PATH = "/data/"
