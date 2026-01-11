# test_unique.py
from rdbms_core import SimpleRDBMS
import os

if os.path.exists("test.db"): os.remove("test.db")
db = SimpleRDBMS("test.db")

print("1. Creating table with UNIQUE constraints...")
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE)")

print("2. Inserting Alice (Success)...")
res = db.execute("INSERT INTO users VALUES (1, 'alice@test.com', 'alice')")
print(res)

print("3. Inserting Bob with DUPLICATE Email (Should Fail)...")
res = db.execute("INSERT INTO users VALUES (2, 'alice@test.com', 'bob')")
print(res)  # Expect Error

print("4. Inserting Charlie with DUPLICATE Username (Should Fail)...")
res = db.execute("INSERT INTO users VALUES (3, 'charlie@test.com', 'alice')")
print(res)  # Expect Error

print("5. Inserting Dave (Success)...")
res = db.execute("INSERT INTO users VALUES (4, 'dave@test.com', 'dave')")
print(res)

print("6. Updating Dave to Alice's email (Should Fail)...")
res = db.execute("UPDATE users SET email = 'alice@test.com' WHERE id = 4")
print(res) # Expect Error