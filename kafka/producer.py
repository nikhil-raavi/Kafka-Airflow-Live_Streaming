from kafka import KafkaProducer
import json
import time
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_generator import generate_transaction, generate_user, generate_product

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka topic
topic = "retail-transactions"

# File to store the last transaction_id
checkpoint_file = "checkpoint.txt"

# Function to read the last transaction ID from the checkpoint file
def get_last_transaction_id():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return int(f.read().strip())
    return 0  # Start from 0 if no checkpoint exists

# Function to save the last transaction ID to the checkpoint file
def save_last_transaction_id(transaction_id):
    with open(checkpoint_file, "w") as f:
        f.write(str(transaction_id))

# Generate sample data
users = [generate_user(i) for i in range(1, 1000)]
products = [generate_product(i) for i in range(1, 300)]

# Get the last transaction ID and resume from there
last_transaction_id = get_last_transaction_id()
print(f"Resuming from transaction ID: {last_transaction_id + 1}")

# Produce messages starting from the last transaction ID
for transaction_id in range(last_transaction_id + 1, last_transaction_id + 10000):  # Generate 10,000 transactions
    transaction = generate_transaction(transaction_id, users, products)
    producer.send(topic, transaction)
    print(f"Sent: {transaction}")
    save_last_transaction_id(transaction_id)  # Save the current transaction ID to the checkpoint
    time.sleep(1)  # Simulate real-time events

producer.flush()
