import os

from dotenv import load_dotenv

load_dotenv()

source_connection = os.getenv('SOURCE_CONNECTION')
destination_connection = os.getenv('DESTINATION_CONNECTION')
base_logging_level = os.getenv('BASE_LOGGING_LEVEL', 'ERROR')
sql_logging_level = os.getenv('SQL_LOGGING_LEVEL', 'ERROR')
