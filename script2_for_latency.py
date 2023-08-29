import asyncio
import random
import aiomqtt as mqtt
import numpy as np
import pandas as pd

import time, json

start_time = 0

async def on_message(client):
    global start_time
    async with client.messages() as messages:
        async for message in messages:
            FTD = json.loads(str(message.payload.decode("utf-8")))
            print(time.time() - start_time)
                
        
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

async def main():
    global start_time, finished
    client = mqtt.Client('broker.hivemq.com', 1883)
        
    await client.connect()
    print("fake client connected")
    await client.subscribe("NP_UNIBO_FTD")
    t0 = asyncio.create_task(on_message(client))
    
    file = "simu_log.log"


    with open(file, 'r') as file:
        for line in file:
            row = json.loads(line)
            topic = row["topic"]
            msg = row["msg"]
            if topic in ["NP_UNIPR_AROUSAL", "Emotions", "RL_VehicleDynamics", "NP_UNITO_DCDC", "AITEK_EVENTS"]:
                await client.publish(topic, json.dumps(msg))
            
            if topic == "NP_UNITO_DCDC":
                start_time = time.time()
                await asyncio.sleep(0.5)
    
    await asyncio.sleep(100)            
    t0.cancel()
    
    await client.disconnect()
                
asyncio.run(main())