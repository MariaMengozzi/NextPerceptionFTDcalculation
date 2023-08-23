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
    notification = False
    last_vd = False
    with open(file, 'r') as file:
        for line in file:
            row = json.loads(line)
            topic = row["topic"]
            msg = row["msg"]
            if topic in ["NP_UNIPR_AROUSAL", "Emotions", "RL_VehicleDynamics", "NP_UNITO_DCDC", "AITEK_EVENTS"]:
                if topic == "AITEK_EVENTS":
                    last_vd = msg["start"] == "true"
                    
                if notification:
                    if topic == "NP_UNITO_DCDC":
                        msg["cognitive_distraction"] = 0
                    elif topic == "RL_VehicleDynamics":
                        if msg['VehicleDynamics']['speed']['x'] > 30:
                            msg['VehicleDynamics']['speed']['x'] = msg['VehicleDynamics']['speed']['x'] - 0.05 #hipotesys about the decrement
                        
                    client.publish(topic, json.dumps(msg))
                else:
                    client.publish(topic, json.dumps(msg))
                
            elif topic == "NP_UNIBO_FTD":
                if msg["person0"]["ftd"] <= 0.75:
                    notification = True
                    if last_vd:
                        last_vd = False
                        client.publish("AITEK_EVENTS", json.dumps(
                            {"start": str(bool(last_vd)).lower(), "event": "One handed", "timestamp": 1674804574914.639}
                        ))
                elif msg["person0"]["ftd"] == 1:
                    notification = False
                
            if topic == "NP_UNITO_DCDC":
                await asyncio.sleep(0.5)
asyncio.run(main())