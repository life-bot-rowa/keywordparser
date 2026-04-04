import os
from dotenv import load_dotenv

load_dotenv()

# --- DataForSEO ---
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD")

# --- Google Ads API ---
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")

# --- Groq (intent classification) ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# --- Telegram (optional) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Filters ---
MIN_VOLUME = int(os.getenv("MIN_VOLUME", "10"))
LANGUAGE = "en"
LOCATION_CODE = 2840  # United States

STOP_WORDS = [
    "porn", "xxx", "nude", "naked", "sex",
    "free download", "torrent", "crack", "keygen",
    "login", "sign in", "log in",
]

# --- Batch sizes ---
DATAFORSEO_BATCH_SIZE = 1000
GROQ_BATCH_SIZE = 500

# --- Paths ---
SEEDS_FILE = "seeds/seeds.txt"
COMPETITORS_FILE = "competitors/competitors.txt"
RAW_DIR = "raw"
OUTPUT_FILE = "output/keywords_final.csv"
