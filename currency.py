import requests
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB')
}

# APIs to fetch rates
apis = {
    'exchangerate': 'https://api.exchangerate-api.com/v4/latest/USD',
    'coingecko': 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd'
}

# Connect to MySQL
def create_connection():
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            logging.info("Connected to MySQL database.")
        return conn
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

# Create tables dynamically
def create_table(conn, table_name):
    try:
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            rate FLOAT NOT NULL
        )"""
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"Table '{table_name}' is ready.")
    except Error as e:
        logging.error(f"Error creating table {table_name}: {e}")

# Save rates to the database
def save_rate(conn, table_name, rate):
    try:
        cursor = conn.cursor()
        insert_query = f"INSERT INTO {table_name} (timestamp, rate) VALUES (%s, %s)"
        cursor.execute(insert_query, (datetime.now(), rate))
        conn.commit()
        logging.info(f"Saved rate to {table_name}: {rate}")
    except Error as e:
        logging.error(f"Error saving rate to {table_name}: {e}")

# Fetch data from APIs
def fetch_data():
    conn = create_connection()
    if not conn:
        return

    for api_type, url in apis.items():
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if api_type == 'exchangerate':
                for currency, rate in data['rates'].items():
                    table_name = f"{api_type}_{currency.lower()}"
                    create_table(conn, table_name)
                    save_rate(conn, table_name, rate)

            elif api_type == 'coingecko':
                for crypto, details in data.items():
                    table_name = f"{api_type}_{crypto}"
                    create_table(conn, table_name)
                    save_rate(conn, table_name, details['usd'])

        except requests.RequestException as e:
            logging.error(f"Error fetching {api_type} data: {e}")
        except KeyError as e:
            logging.error(f"Unexpected data format for {api_type}: {e}")

    conn.close()

# Scheduler to run daily at midnight
def schedule_task():
    scheduler = BlockingScheduler()
    scheduler.add_job(fetch_data, 'cron', hour=0, minute=0)
    try:
        logging.info("Scheduler started.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")

if __name__ == '__main__':
    fetch_data()  # Run once initially
    schedule_task()
