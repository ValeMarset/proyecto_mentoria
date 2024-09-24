from config.ConnectionDb import ConnectionDb
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT'),
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'database': os.getenv('DATABASE')
}

db_connection = ConnectionDb(**db_config)
db_connection.connect()
db_connection.test_connection()
db_connection.insert_records('etl_orders.pepe', ['nombre'], [('Valentitina',)])
db_connection.close()
