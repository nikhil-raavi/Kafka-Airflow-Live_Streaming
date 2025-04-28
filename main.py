from utils.data_generator import generate_user, generate_product, generate_transaction

# Generate users and products
users = [generate_user(i) for i in range(1, 1000)]  # Generate 1000 users
products = [generate_product(i) for i in range(1, 300)]  # Generate 300 products

# Generate transactions
transactions = [generate_transaction(i, users, products) for i in range(1, 10000)]  # Generate 20 transactions

print("Sample Users:", users[0])
print("Sample Products:", products[0])
print("Sample Transactions:", transactions[0])
