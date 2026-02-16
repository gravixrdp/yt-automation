
import os
import sys
# Mock potential missing env vars if needed, but better to load them.
# We will use scheduler_config to load them.
sys.path.append("/home/ubuntu/gravix-agent")
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/gravix-agent/.env")
import scheduler_config
import oauth_helper

print("--- ENV VARS ---")
print(f"YOUTUBE_CLIENT_ID: {scheduler_config.YOUTUBE_CLIENT_ID}")
print(f"YOUTUBE_REDIRECT_URI: {scheduler_config.YOUTUBE_REDIRECT_URI}")
print("----------------")

url, state = oauth_helper.generate_youtube_oauth_url()
print("\n--- GENERATED URL ---")
print(url)
print("---------------------")
