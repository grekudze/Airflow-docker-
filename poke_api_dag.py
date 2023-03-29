import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}
def get_pokemon():
    url = "https://pokeapi.co/api/v2/pokemon?limit=2000&offset=0"
    response = requests.get(url)


    pokemons = response.json()['results']
    for pokemon in pokemons:
        response = requests.get(pokemon['url'])
        if response.ok:
            pokemon_data = response.json()
            name = pokemon_data['name']
            weight = pokemon_data['weight']
            types = [t['type']['name'] for t in pokemon_data['types']]
            print(f"Name: {name}")
            print(f"Weight: {weight}")
            print(f"Types: {types}")


with DAG('poke_api_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    get_pokemon_task = PythonOperator(
        task_id='get_pokemon',
        python_callable=get_pokemon
    )
