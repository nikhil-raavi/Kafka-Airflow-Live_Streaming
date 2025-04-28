import boto3 # this is used to authenticate AWS credentials which is stored in ur local system
import json
from kafka import KafkaConsumer

# AWS S3 setup
s3_client = boto3.client('s3')
bucket_name = 'nikhil-kafka-data-2024'

# Kafka Consumer setup
print("Setting up Kafka consumer...")
consumer = KafkaConsumer(
    'retail-transactions',  # Topic name
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("Kafka consumer is now listening...")

# Batch size and buffer
batch_size = 1000
batch = []

# Process Kafka messages in batches
for message in consumer:
    print(f"Received message: {message.value}")  # Log received messages
    batch.append(message.value)

    if len(batch) >= batch_size:
        file_name = f"transactions/batch_{batch[0]['timestamp'].replace(':', '-')}_to_{batch[-1]['timestamp'].replace(':', '-')}.json"
        print(f"Uploading batch to S3: {file_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(batch)
        )
        print(f"Uploaded {file_name} with {len(batch)} records to S3")
        batch = []  # Clear the batch buffer

# Handle any remaining messages in the batch
if batch:
    file_name = f"transactions/batch_{batch[0]['timestamp'].replace(':', '-')}_to_{batch[-1]['timestamp'].replace(':', '-')}.json"
    print(f"Uploading final batch to S3: {file_name}")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(batch)
    )
    print(f"Uploaded final {file_name} with {len(batch)} records to S3")
