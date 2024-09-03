import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
DATABASE_URL = "sqlite:///database.db"
JSON_FILE = 'round-pen-404209-78c5f6e5c9ea.json'
ALLOWED_IDS = [514900377, 781710702]
