#client bloccante: se tutte le operazioni sono sequenziali possiamo usare questo client single threaded.

import paho.mqtt.client as paho
import pandas as pd
import json
import datetime
import os
import logging

logging.basicConfig(filename=os.path.dirname(os.path.abspath(__file__))+'/client.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

class BrokerNameException(Exception):
    """Raised when the broker name is none or empty """
    def __init__(self, message="the broker name is none or empty"):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)

class PortNumberException(Exception):
    """Raised when the port number is none"""
    def __init__(self, message="the port number is none"):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)

class EmptyMessageException(Exception):
    """Raised when the message is empty"""
    def __init__(self, topic, message="the message is empty"):
        self.topic = topic
        self.message = self.topic +': '+ message
        logger.error(self.message)
        super().__init__(self.message)



#initialize weights and variable

FTD_MAX_PUBLISH = 1

FTD = 1

IDC = 0
IDV = 0
threshold_i_c = 1
threshold_i_v = 2
weight = 1.01
threshold_v = 300
decimals = 4
weight_anger = 0.125
weight_happiness = 0.125
weight_fear = 0.083
weight_sadness = 0.083
weight_neutral = 0
weight_disgust = 0.042
weight_sorprise = 0.042
weights_emozioni = pd.Series([weight_anger, weight_happiness, weight_fear, weight_sadness, weight_neutral, weight_disgust, weight_sorprise])
s = 0
Ei = 0
DCi = 0
DVi = 0

flagE = False
flagD = False
flagV = False


#buffer for mean insert
anger = []
happiness = []
fear = []
sadness = []
neutral = []
disgust = []
surprise = []
DCs = []
DVs = []
ss = []
DVis = []
DCis = []
emotions_total = []
FTDs = []

count = 0

user = 'person0'

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_message(client, userdata, msg):
    global FTD, IDC, IDV, weight, decimals, threshold_v, threshold_i_v, threshold_i_c, DCi, DVi, s, Ei, flagD, flagE, flagV
    global DCs, DVs, DCis, DVis, emotions_total, count, anger, happiness, fear, sadness, neutral, disgust, surprise, FTDs, ss
    global user
    #print("topic: "+msg.topic)

    if msg.topic == 'NP_RELAB_VD':
        s = json.loads(str(msg.payload.decode("utf-8")))['VehicleDynamics']['speed']['x']
        ss.append(s)
        flagV = True
    elif msg.topic == 'NP_UNITO_DCDC':
        try:
            if len(str(msg.payload.decode('utf-8'))) == 0:
                raise EmptyMessageException(topic='NP_UNITO_DCDC')
            else:
                D = json.loads(str(msg.payload.decode("utf-8")))

                cd = D['cognitive_distraction'] if D['cognitive_distraction_confidence'] != 0 else 0.0
                if D['cognitive_distraction_confidence'] == 0.0:
                    logger.warning('NO cognitive distraction value')
                    print('NO cognitive distraction value')

                vd = D['eyesOffRoad'] if D['eyesOffRoad_confidence'] != 0.0 else 0.0
                if D['eyesOffRoad_confidence'] == 0.0:
                    logger.warning('NO visual distraction value')
                    print('NO visual distraction value')
        except Exception as exception:
            cd = 0.0
            vd = 0.0
            print(exception)

        DCi = round(cd * s/threshold_v * weight **(IDC - threshold_i_c), decimals)
        DVi = round(vd * s/threshold_v * weight **(IDV - threshold_i_v), decimals)

        flagD = True
        DCs.append(cd)
        DVs.append(vd)
        DCis.append(DCi)
        DVis.append(DVi)

    elif msg.topic == 'Emotions':
        try:
            if len(str(msg.payload.decode('utf-8'))) == 0:
                e = {"predominant" : "0","neutral":"0","happiness": "0","surprise":"0","sadness": "0","anger": "0","disgust": "0","fear": "0","engagement": "0","valence": "0"}
                raise EmptyMessageException(topic='Emotions')

            if len(json.loads(str(msg.payload.decode("utf-8")))) == 0:
                e = {"predominant" : "0","neutral":"0","happiness": "0","surprise":"0","sadness": "0","anger": "0","disgust": "0","fear": "0","engagement": "0","valence": "0"}
                logger.warning('NO emotion value')
                print('NO emotion value')
            else:
                e = json.loads(str(msg.payload.decode("utf-8")))[user]
        except Exception as exception:
                print(exception)

        emotions = pd.Series([float(e['anger']), 
                            float(e['happiness']), float(e['fear']), 
                            float(e['sadness']), float(e['neutral']), 
                            float(e['disgust']), float(e['surprise'])
                        ])
        
        Ei =  round((emotions * weights_emozioni).sum() / weights_emozioni.sum(), decimals)

        flagE = True
        anger.append(float(e['anger']))
        happiness.append(float(e['happiness']))
        fear.append(float(e['fear']))
        sadness.append(float(e['sadness']))
        neutral.append(float(e['neutral']))
        disgust.append(float(e['disgust']))
        surprise.append(float(e['surprise']))
        emotions_total.append(Ei)
    elif msg.topic == 'NP_UNIBO_FTD':
        FTD = json.loads(str(msg.payload.decode("utf-8")))['person0']['ftd']

    if flagE and flagD and flagV:
        ftd = {user:{
            'timestamp': datetime.datetime.now().timestamp(),
            'ftd' : max(0, 1 - (DCi + DVi + Ei))
            }}
        client.publish("NP_UNIBO_FTD", json.dumps(ftd))
        flagE = False
        flagD = False
        flagV = False
        FTDs.append(max(0, 1 - (DCi + DVi + Ei)))
        print()
        if count == FTD_MAX_PUBLISH:
            d = {
            'speed': ss,
            'DC':DCs,
            'DCis': DCis,
            'DV':DVs,
            'DVis':DVis,
            'anger' : anger,
            'disgust' : disgust,
            'fear' : fear,
            'happiness' : happiness,
            'neutral' : neutral,
            'sadness' : sadness,
            'surprise' : surprise,
            'emotions_total': emotions_total,
            'FTD' : FTDs,
            }

            data = pd.DataFrame(d)
            data_mean = pd.DataFrame(d).mean()

            speed_mean= data_mean['speed']
            visual_distraction_mean = 1 if data['DV'].sum() > len(DVs)//2 else 0
            cognitive_distraction_mean = 1 if data['DC'].sum() > len(DCs)//2 else 0
            anger_mean = data_mean['anger']
            happiness_mean = data_mean['happiness']
            fear_mean = data_mean['fear']
            sadness_mean = data_mean['sadness']
            disgust_mean = data_mean['disgust']
            surprise_mean = data_mean['surprise']
            neutral_mean = data_mean['neutral']
            visual_distraction_tot_mean = data_mean['DVis']
            cognitive_distraction_tot_mean = data_mean['DCis']
            emozione_tot = data_mean['emotions_total']
            ftd_tot =  data_mean['FTD']
            
            #TODO INSERT INTO DATABASE
            print('INSERT INTO DATABASE:')
            print(f'''
            user: {user}
            speed = {speed_mean}
            visual_distraction = {visual_distraction_mean}
            cognitive_distraction = {cognitive_distraction_mean}
            anger = {anger_mean}
            happiness = {happiness_mean}
            fear = {fear_mean}
            sadness = {sadness_mean}
            disgust = {disgust_mean}
            surprise = {surprise_mean}
            neutral = {neutral_mean}
            visual_distraction_tot = {visual_distraction_tot_mean}
            cognitive_distraction_tot = {cognitive_distraction_tot_mean}
            emoziones_tot = {emozione_tot}
            ftd_tot =  {ftd_tot}
            ''')

            anger = []
            happiness = []
            fear = []
            sadness = []
            neutral = []
            disgust = []
            surprise = []
            DCs = []
            DVs = []
            ss = []
            DVis = []
            DCis = []
            emotions_total = []
            FTDs = []
            count = 0 
        else:
            count +=1

def main():
    logger.info('')
    logger.info('new client start')
    broker_name = None #'tools.lysis-iot.com'
    port = None #1883

    try:

        with open((os.path.dirname(os.path.abspath(__file__)) +'/config.json').replace ('\\', '/'),'r') as json_file:
            data = json.load(json_file)['client_mqtt_config']
            print(data['broker_name'])
            broker_name = data['broker_name']
            print(data['port'])
            port = int(data['port'])
            
    except Exception as exception:
        print(exception)



    try:
        if broker_name is None:
            raise BrokerNameException
        elif port is None:
            raise PortNumberException

        client = paho.Client()
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.connect(broker_name, port) 
        client.subscribe('NP_UNITO_DCDC', qos=1)
        client.subscribe('Emotions', qos=1)
        client.subscribe('NP_RELAB_VD', qos=1)# Effective speed
        client.loop_forever()
    except Exception as exception:
        print('connect to client error')
        print(exception)

    


if __name__=="__main__":
    main()