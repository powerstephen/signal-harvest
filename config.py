import os
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")
DB_PATH = os.getenv("DB_PATH", "/tmp/prospect.db")
DB_URL = f"sqlite:///{DB_PATH}"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
