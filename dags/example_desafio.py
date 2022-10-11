import sqlite3
import os
import pandas as pd
#from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
#from textwrap import dedent
#from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

FILE_PATH = os.path.abspath(__file__)
PROJECT_PATH = os.path.dirname(os.path.dirname(FILE_PATH))
DB_PATH = os.path.join(PROJECT_PATH, 'data/Northwind_small.sqlite')

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_nw_orders():

    conn = sqlite3.connect(DB_PATH, isolation_level=None,
                           detect_types=sqlite3.PARSE_COLNAMES)
    df = pd.read_sql_query('select * from "Order"', conn)
    df.to_csv('output_orders.csv', index=False)
    
    return None

def read_nw_od():
    
    conn = sqlite3.connect(DB_PATH, isolation_level=None,
                           detect_types=sqlite3.PARSE_COLNAMES)
    df_od = pd.read_sql_query('select * from "OrderDetail"', conn)
    df_o = pd.read_csv('/home/lucas/indicium/Desafio5/output_orders.csv')

    df = pd.merge(df_od, df_o, left_on='OrderId', right_on='Id', how='left')
    
    soma = df[df['ShipCity']=='Rio de Janeiro']['Quantity'].sum()

    with open('count.txt', 'w+') as f:
        f.write(str(soma))

    return None

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    write_output_orders = PythonOperator(
        task_id='write_output_orders',
        python_callable=read_nw_orders,
        provide_context=True
    )

    write_count = PythonOperator(
    task_id='write_count',
    python_callable=read_nw_od,
    provide_context=True
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    write_output_orders >> write_count >> export_final_output