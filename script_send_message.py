import asyncio
import random
import paho.mqtt.client as paho
import numpy as np

import time, json

async def main():
    decimals = 4
    start = 0
    client = paho.Client()
    #client.on_subscribe = on_subscribe
    #client.on_message = on_message
    client.connect('broker.hivemq.com', 1883) #tools.lysis-iot.com -> broker prof

    def speed_function(time):
        time_seconds = time * 60  # Conversione in secondi
        if 0 <= time_seconds <= 20:
            return 30
        elif 20 < time_seconds <= 35:
            return 30 + (time_seconds - 20) * (55 - 30) / (35 - 20)
        elif 35 < time_seconds <= 43:
            return 55 - (time_seconds - 35) * (55 - 30) / (43 - 35)
        else:
            return 30

    def cd_function(t):
        if 0 <= t <= 0.25 or t >= 0.6:
            return 0
        elif 0.25 < t <= 0.35:
            return 0
        elif 0.35 < t < 0.6:
            return 1

    def vd_function(t):
        if 0 <= t <= 0.25 or t >= 0.6:
            return 0
        elif 0.25 < t <= 0.4:
            return 1
        elif 0.4 < t < 0.6:
            return 0

    time_points = np.linspace(0, 2, num=240) #quattro campionamenti di velocità al secondo
    speed_values = [speed_function(t) for t in time_points]

    cd_val = [cd_function(t) for t in time_points[0::4]]
    vd_val = [vd_function(t) for t in time_points[0::4]]

    cd_val = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    vd_val = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for i in range(0, len(speed_values)-1):
        anger = round(random.random(),decimals) # num casuale tra 0 e 1
        disgust = round(random.uniform(0, 1-anger), decimals)
        fear = round(random.uniform(0, 1-(anger + disgust)), decimals)
        joy = round(random.uniform(0, 1-(anger + disgust + fear)), decimals)
        neutral = round(random.uniform(0, 1-(anger + disgust + fear + joy)), decimals)
        sadness = round(random.uniform(0, 1-(anger + disgust + fear + joy + neutral)), decimals)
        surprise = round(1 - (anger + disgust + fear + joy + neutral + sadness), decimals)

        emotion_topic = ['{"person0" : {"predominant" : "0","neutral":"'+ str(neutral)+'","happiness": "'+str(joy)+'","surprise":"'+str(surprise)+'","sadness": "'+str(sadness)+'","anger": "'+str(anger)+'","disgust": "'+str(disgust)+'","fear": "'+str(fear)+'","engagement": "4.4877","valence": "0.0154492"} } ']
        emotion = random.choice(emotion_topic)
        #speed = '{"Effective speed":'+str(s)+'}'
        speed = '{"VehicleDynamics": { "COGPos": { "x": 2674.6463496370525, "y": -14.564664744839954, "z": 0.45894402610593055 }, "GearBoxMode": 10, "acceleration": { "heading": -0.0033882581628859043, "pitch": 1.0706313332775608e-05, "roll": -0.0091178948059678078, "x": 0.28764855861663818, "y": 0.11754922568798065, "z": 0.0011688719969242811 }, "accelerator": 1, "brake": 0, "clutch": 0, "engineSpeed": 397.5355224609375, "engineStatus": 1, "gearEngaged": 6, "position": { "heading": -2.4471809390732866, "pitch": 0.0010818612183405301, "roll": 0.0064789495254821217, "x": 2675.8232267297335, "y": -13.584974385944776, "z": -0.0013803915935547695 }, "radarInfos": { "angle": 1110634.625, "anglesx": 0, "anglesy": 0, "anglesz": 0, "azimuth": 0, "distanceToCollision": 0, "id": 0, "laneId": 0, "posx": 0, "posy": 0, "posz": 0, "roadId": 0, "speed": 0, "visibility": 0 }, "roadInfo": { "intersectionId": -1, "laneGap": -0.049474708735942841, "laneId": 2, "roadAbscissa": 679.15997314453125,  "roadAngle": -3.141146183013916, "roadGap”": 1.7005252838134766, "roadId": 37 }, "speed": { "heading": 0.003960845060646534, "pitch": 6.9768051616847515e-05, "roll": 0.0023608640767633915,  "x": '+ str(speed_values[i]) +', "y": -0.029801525175571442, "z": 0.054808627814054489 }, "steeringTorq": -0.052799351513385773, "steeringWheelAngle": 0.0067141000181436539, "steeringWheelSpeed": -0.016700951382517815,  "timestamp": 1631180995458, "wheelAngle": 0.00025969580747187138 }, "wheelState": { "0": { "angle": 1110634.625, "grip": 1,"laneType": 3, "posx": 2.6995155811309814, "posy": 0.75018519163131714, "posz": 0.3011646568775177, "rotx": 0.00028212839970365167, "roty": -0.060661893337965012, "rotz": 0.0025958430487662554, "speed": 161.23155212402344, "vhlDelta": -0.0030761519446969032, "vhlSx": 0.069431886076927185 }, "1": { "angle": 1109191, "grip": 1, "laneType": 3, "posx": 2.6995253562927246, "posy": -0.75022125244140625, "posz": 0.30186328291893005, "rotx": -0.00028936716262251139, "roty": -0.060574695467948914, "rotz": -0.0020764514338225126, "speed": 161.22161865234375, "vhlDelta": 0.0015965558122843504, "vhlSx": 0.0067795705981552601 }, "2": { "angle": 1099277.125, "grip": 1, "laneType": 3, "posx": -0.0020916683133691549, "posy": 0.74077975749969482, "posz": 0.30334118008613586, "rotx": 0.0019761791918426752, "roty": -0.13483227789402008, "rotz": -0.0015447111800312996, "speed": 160.06707763671875, "vhlDelta": 0.00085329683497548103, "vhlSx": -0.00049092137487605214 }, "3": { "angle": 1101188, "grip": 1, "laneType": 3, "posx": -0.002209091791883111, "posy": -0.74075996875762939, "posz": 0.30403971672058105, "rotx": -0.0021273412276059389, "roty": -0.13346090912818909, "rotz": 0.0015314865158870816, "speed": 160.07774353027344, "vhlDelta": -0.022224732674658298, "vhlSx": -0.00048526551108807325 } } }'
        arousal_topic = [
            json.dumps({"arousal": 0}),
            #json.dumps({"arousal": round(random.random(), decimals)}),
            #json.dumps({"arousal": 1}),
            #json.dumps({"arousal": -1})
        ]
        arousal = random.choice(arousal_topic)
        client.publish('NP_UNIPR_AROUSAL',arousal)
        client.publish('Emotions', emotion)
        client.publish('RL_VehicleDynamics', speed)
            
        if i % 4 == 0:
            DC = cd_val[int(i/4)]
            DV = vd_val[int(i/4)]
            
            DC_topic = ['{"time": 123456, "eyesOffRoad": ' +str(1)+',"cognitive_distraction":'+str(DC)+', "eyesOffRoad_confidence": '+str(1)+',  "cognitive_distraction_confidence": '+str(1)+', "eyesOffRoad_pred_1s": 0.0, "cognitive_distraction_pred_1s": 0.0 }']
            D = random.choice(DC_topic)
            client.publish('NP_UNITO_DCDC', D)
            

            if DV != start:
                start = DV
            DV_topic = '{"timestamp": "2022-04-11 16:52:26.123", "event": "reverse", "start": '+ str(bool(DV)).lower() + '}'

            client.publish('AITEK_EVENTS', DV_topic)

            await asyncio.sleep(1)
    
    

asyncio.run(main())