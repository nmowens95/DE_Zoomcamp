import os
from dotenv import load_dotenv


load_dotenv()

user = os.getenv('USER')
password = os.getenv('PASSWORD')
host= os.getenv('HOST')
db = os.getenv('DB')
port = os.getenv('PORT')

print(user, password, host, db, port)