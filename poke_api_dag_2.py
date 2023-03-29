import requests
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

def get_pokemon():
    url = "https://pokeapi.co/api/v2/pokemon?limit=2000&offset=0"
    response = requests.get(url)

    pokemons = response.json()['results']
    result = []
    for pokemon in pokemons:
        response = requests.get(pokemon['url'])
        if response.ok:
            pokemon_data = response.json()
            name = pokemon_data['name']
            weight = pokemon_data['weight']
            types = [t['type']['name'] for t in pokemon_data['types']]
            result.append((name, weight, types))

    return result

def load_to_postgres(**context):
    # Получение результата из контекста
    data = context['task_instance'].xcom_pull(task_ids='get_pokemon')

    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="1:5432"
    )

    # Создание таблицы, если она не существует
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS airflow")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pokemon (
            name TEXT,
            weight FLOAT,
            types TEXT[]
        )
    """)

    # Загрузка данных в PostgreSQL
    cursor = conn.cursor()
    for row in data:
        cursor.execute("INSERT INTO pokemon (name, weight, types) VALUES (%s, %s, %s)", row)
    conn.commit()
    cursor.close()
    conn.close()

with DAG('poke_api_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    get_pokemon_task = PythonOperator(
        task_id='get_pokemon',
        python_callable=get_pokemon,
        provide_context=True
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    # Определение зависимостей
    get_pokemon_task >> load_to_postgres_task
