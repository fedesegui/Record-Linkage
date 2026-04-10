import os
from sqlalchemy import create_engine
import sys
from dotenv import load_dotenv

parent_dir = os.path.abspath(os.path.join(os.getcwd(), r'C:\Users\pdubourdieu\Documents\Repositorios\main\ine'))
sys.path.append(parent_dir)
dotenv_path = os.path.join(parent_dir, '.env')
load_dotenv(dotenv_path)

def make_dw_conection():
    user = os.getenv('DB_USER_ENEC')
    password = os.getenv('DB_PASSWORD_ENEC')
    host = '10.231.30.63'
    port = 5432
    db_name = 'RRAA'
    url = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
    engine = create_engine(url)
    conn = engine.connect()
    return conn
    