import asyncio
import random
import paho.mqtt.client as paho
import numpy as np
import pandas as pd

import time, json

async def main():
    client = paho.Client()
    #client.on_subscribe = on_subscribe
    #client.on_message = on_message
    client.connect('broker.hivemq.com', 1883) #tools.lysis-iot.com -> broker prof
    file = "simu_log.log"
    with open(file, 'r') as file:
        for line in file:
            row = json.loads(line)
            topic = row["topic"]
            msg = row["msg"]
            if topic in ["NP_UNIPR_AROUSAL", "Emotions", "RL_VehicleDynamics", "NP_UNITO_DCDC", "AITEK_EVENTS"]:
                client.publish(topic, json.dumps(msg))
            
            if topic == "NP_UNITO_DCDC":
                await asyncio.sleep(0.5)
asyncio.run(main())