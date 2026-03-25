import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# Configuration
TOPIC_NAME = 'weather-data'
files = ['synop_2006.csv', 'synop_2007.csv', 'synop_2008.csv', 'synop_2009.csv', 'synop_2010.csv']

print("Tentative de connexion à Kafka...")
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(0, 10, 1),
            request_timeout_ms=60000
        )
    except Exception as e:
        print("Kafka n'est pas encore prêt, attente de 5 secondes...")
        time.sleep(5)

def stream_csv(file_path):
    if not os.path.exists(file_path):
        print(f"Erreur : Le fichier {file_path} est introuvable !")
        return

    print(f"Lecture et envoi de : {file_path}")
    df = pd.read_csv(file_path, sep=';')
    
    for _, row in df.iterrows():
        message = row.to_dict()
        clean_message = {k: (None if pd.isna(v) else v) for k, v in message.items()}
        producer.send(TOPIC_NAME, value=clean_message)
        # On envoie 20 messages par seconde pour ne pas saturer 
        time.sleep(0.05)

if __name__ == "__main__":
    for f in files:
        stream_csv(f)
