from kafka import KafkaProducer
import json
import time

# Create producer
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',  # Changed from localhost:9092
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Send 10 messages
for i in range(10):
    message = {
        'id': i,
        'name': f'Message {i}',
        'value': i * 100
    }
    
    producer.send('test-topic', value=message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.close()
print("Done sending messages!")



