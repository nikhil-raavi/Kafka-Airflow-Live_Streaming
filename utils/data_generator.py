from faker import Faker
import random

fake = Faker()

def generate_user(user_id):
    return {
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "country": fake.country()
    }

def generate_product(product_id):
    return {
        "product_id": product_id,
        "name": fake.word(),
        "category": random.choice(["Electronics", "Clothing", "Grocery"]),
        "price": round(random.uniform(5.0, 100.0), 2)
    }

def generate_transaction(transaction_id, users, products):
    user = random.choice(users)
    product = random.choice(products)
    return {
        "transaction_id": transaction_id,
        "user_id": user["user_id"],
        "product_id": product["product_id"],
        "quantity": random.randint(1, 5),
        "total_price": round(product["price"] * random.randint(1, 5), 2),
        "timestamp": fake.date_time_this_month().isoformat()
    }
