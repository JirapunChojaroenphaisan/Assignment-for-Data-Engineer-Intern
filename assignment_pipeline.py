'''
นำ assignment interview 
ไปทำ data pipeline airflow เก็บใน
data lake (google cloud storage)
และ upload ไปยัง datawarehouse (google Bigquery)
'''
#import library 
import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# api
url_randomuser = "https://randomuser.me/api"


# path ที่จะใช้
final_output_path = "/home/airflow/gcs/data/final.csv"
randomuser_output_path = "/home/airflow/gcs/data/data_user.csv"
gender_output_path = "/home/airflow/gcs/data/data_gender.csv"

def get_data_from_api(randomuser):
    #นำข้อมูลจากมา api มา 20 รอบ
    df = pd.DataFrame()

    response = requests.get(url_randomuser, params={'results': 20})
    data = response.json()
    df = pd.json_normalize(data['results'])

    
    df.to_csv(randomuser, index=False)
    print(f"output to {randomuser}")

def get_data_api_compare_name(randomuser,gender):
    #นำข้อมูลแรก มาเปรียบเทียบกับ api 
    randomuser = pd.read_csv(randomuser)
    df_gender = pd.DataFrame()

    data_list = []

    for index, row in randomuser.iterrows():
        name = row['name.first']
        url = f"https://api.genderize.io/?name={name}"
        response = requests.get(url)
        data = response.json()
        data_list.append(data)

    df_gender = pd.DataFrame.from_records(data_list)
    
    df_gender.to_csv(gender, index =False)
    print(f"output to {gender}")


def merge_data (randomuser,gender,final):
    #นำข้อมูลทั้ง 2  มารวมกัน
    randomuser = pd.read_csv(randomuser)
    gender = pd.read_csv(gender)
    
    df_final = randomuser.merge(gender,left_on="name.first",right_on='name')

    #นำข้อมูลเพศ มาเปรียบเทียบกันความถูกต้องและสร้างเป็น columns ใหม่
    df_final['same_gender'] = df_final['gender_x'] == df_final['gender_y']

    #จัดแต่งให้ดูสวยงามพร้อม save ไฟล์
    df_final = df_final.rename(columns={'name.first': 'first_name', 'name.last': 'last_name', 'gender_x': 'gender(actual)','gender_y': 'gender(predict)'})
    df_final = df_final[['first_name','last_name','gender(actual)','gender(predict)','probability','same_gender']]
    
    df_final.to_csv(final, index = False)
    print(f"output to {final}")

with DAG(
    "final_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["Bonus"]
) as dag:
    
    t1 = PythonOperator(
        task_id = "get_data_from_api",
        python_callable=get_data_from_api,
        op_kwargs={
        "randomuser":randomuser_output_path
        },
    )
    
    t2 = PythonOperator(
        task_id = "compare_data_gender_from_api",
        python_callable = get_data_api_compare_name,
        op_kwargs={
        "randomuser":randomuser_output_path,
        "gender":gender_output_path

        }
    )
    
    t3 = PythonOperator(
        task_id = "merge_data_and_compare_gender",
        python_callable = merge_data,
        op_kwargs={
        "randomuser":randomuser_output_path,
        "gender":gender_output_path,
        "final":final_output_path

        }
        
    )

    t4 = GCSToBigQueryOperator(
        
    )

    t1 >> t2 >> t3
