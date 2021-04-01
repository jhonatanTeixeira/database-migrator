import os

from dotenv import load_dotenv

load_dotenv()

source_connection = os.getenv('SOURCE_CONNECTION')
destination_connection = os.getenv('DESTINATION_CONNECTION')