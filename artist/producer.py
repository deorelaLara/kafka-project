
from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='vocal-hog-5938-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='dm9jYWwtaG9nLTU5MzgkQW_khQF3X3_0WftGxYp_DwV5AsK3G7pOiVhnGpbtveg',
    sasl_plain_password='NGFkOTk2YmMtYjNjNy00OTg5LTg3MzMtYjFmN2ExNzgxNjBk',
    api_version_auto_timeout_ms=1000000,
)

tracks = pd.read_csv('atists.csv')

for dt in tracks.to_dict(orient='records'):
    data = json.dumps(dt).encode('utf-8')

    try:
        result = producer.send('artist', data).get(timeout = 60)    
        print("Message produced:", result)
    except Exception as e:
        print(f"Error producing message: {e}")
producer.close()