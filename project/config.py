from dotenv import load_dotenv
import os

load_dotenv()


DB_CONFIG = {
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASS'),
    "database": os.environ.get('DB_NAME'),
    "host": os.environ.get('DB_HOST')
}