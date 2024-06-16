from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes = 3)
}

def load_data_from_api(endpoint, params):
    url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{endpoint}"
    headers = {
        "X-Nickname": "deb",
        "X-Cohort": "25",
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def extract_restaurant_data(**kwargs):
    end_date = datetime.now()
    #start_date = end_date - timedelta(days = 7)
    start_date = datetime(2024, 1, 1)
    limit = 50
    offset = 0
    data = []
    while True:
        response = load_data_from_api("restaurants", 
                                    {
                                        "sort_field": "_id", 
                                        "sort_direction": "asc", 
                                        "start_date": start_date.strftime('%Y-%m-%d'), 
                                        "end_date": end_date.strftime('%Y-%m-%d'), 
                                        "limit": limit, 
                                        "offset": offset
                                        })
        if not response:
            break 
        for item in response:
            if 'address' in item:
                item['address'] = item['name']
        data.extend(response)
        offset += limit
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='restaurant_data', value=data)

def extract_courier_data(**kwargs):
    end_date = datetime.now()
    #start_date = end_date - timedelta(days = 7)
    start_date = datetime(2024, 1, 1)
    limit = 50
    offset = 0
    data = []
    while True:
        response = load_data_from_api("couriers", 
                                      {
                                          "sort_field": "_id", 
                                          "sort_direction": "asc", 
                                          "start_date": start_date.strftime('%Y-%m-%d'), 
                                          "end_date": end_date.strftime('%Y-%m-%d'), 
                                          "limit": limit, 
                                          "offset": offset
                                          })
        if not response:
            break
        for item in response:
            if 'address' in item:
                item['address'] = item['name']
        data.extend(response)
        offset += limit
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='courier_data', value=data)

def extract_delivery_data(**kwargs):
    end_date = datetime.now()
    #start_date = end_date - timedelta(days = 7)
    start_date = datetime(2024, 1, 1)
    limit = 50
    offset = 0
    data = []
    while True:
        response = load_data_from_api("deliveries", 
                                      {
                                          "sort_field": "order_ts", 
                                          "sort_direction": "asc", 
                                          "start_date": start_date.strftime('%Y-%m-%d'), 
                                          "end_date": end_date.strftime('%Y-%m-%d'), 
                                          "limit": limit, 
                                          "offset": offset
                                          })
        if not response:
            break
        for item in response:
            if 'address' in item:
                item['address'] = item['address']
        data.extend(response)
        offset += limit
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='delivery_data', value=data)

def insert_data_to_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    task_instance = kwargs['task_instance']
    
    restaurants_data = task_instance.xcom_pull(task_ids='extract_restaurant_data', key='restaurant_data')
    couriers_data = task_instance.xcom_pull(task_ids='extract_courier_data', key='courier_data')
    deliveries_data = task_instance.xcom_pull(task_ids='extract_delivery_data', key='delivery_data')
    

    # Функция проверки наличия идентификатора в таблице
    def check_id_exists(table, id_field, id_value):
        records = pg_hook.get_records(f"SELECT 1 FROM {table} WHERE {id_field} = %s", (id_value,))
        return len(records) > 0

    # Insert данные, если идентификатор не существует
    for restaurant in restaurants_data:
        if not check_id_exists('stg.api_restaurants', 'restaurants_id', restaurant['_id']):
            pg_hook.run(
                """
                INSERT INTO stg.api_restaurants (restaurants_id, object_value) 
                VALUES (%s, %s)
                """, 
                parameters=(restaurant['_id'], json.dumps(restaurant, ensure_ascii=False)))
    
    for courier in couriers_data:
        if not check_id_exists('stg.api_couriers', 'couriers_id', courier['_id']):
            pg_hook.run(
                """
                INSERT INTO stg.api_couriers (couriers_id, object_value) 
                VALUES (%s, %s)
                """, 
                parameters=(courier['_id'], json.dumps(courier, ensure_ascii=False)))
    
    for delivery in deliveries_data:
        if not check_id_exists('stg.api_deliveries', 'delivery_id', delivery['delivery_id']):
            pg_hook.run(
                """
                INSERT INTO stg.api_deliveries (delivery_id, object_value, delivery_ts) 
                VALUES (%s, %s, %s)
                """, 
                parameters=(delivery['delivery_id'], json.dumps(delivery, ensure_ascii=False), delivery['delivery_ts']))

dag = DAG('load_api_data_to_postgres', schedule_interval='@hourly', default_args=default_args, catchup=False)

extract_restaurant_data_task = PythonOperator(
    task_id='extract_restaurant_data',
    python_callable=extract_restaurant_data,
    dag=dag
)

extract_courier_data_task = PythonOperator(
    task_id='extract_courier_data',
    python_callable=extract_courier_data,
    dag=dag
)

extract_delivery_data_task = PythonOperator(
    task_id='extract_delivery_data',
    python_callable=extract_delivery_data,
    dag=dag
)

insert_data_to_postgres_task = PythonOperator(
    task_id='insert_data_to_postgres',
    python_callable=insert_data_to_postgres,
    dag=dag
)

([extract_restaurant_data_task, extract_courier_data_task, extract_delivery_data_task] >> insert_data_to_postgres_task)