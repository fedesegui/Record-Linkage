import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)


def make_dw_conection(db):
    user = os.getenv("DB_USER_REGISTROS")
    password = os.getenv("DB_PASSWORD_REGISTROS")
    host = "svrpostgreugc.ine.gub.uy"
    port = 5432
    db_name = db
    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    conn = engine.connect()
    return conn


def make_test_dw_conection():
    user = os.getenv("DB_USER_REGISTROS")
    password = os.getenv("DB_PASSWORD_REGISTROS")
    host = "svrpostgreugc.ine.gub.uy"
    port = 5432
    db_name = "registros_test"
    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    conn = engine.connect()
    return conn


def make_staging_dw_conection():
    user = os.getenv("DB_USER_REGISTROS")
    password = os.getenv("DB_PASSWORD_REGISTROS")
    host = "svrpostgreugc.ine.gub.uy"
    port = 5432
    db_name = "registros_staging"
    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    conn = engine.connect()
    return conn
