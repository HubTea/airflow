from datetime import datetime
import os
from typing import cast

from requests import request, Response
from pendulum import DateTime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

origin: str = 'https://opendart.fss.or.kr'
company_code_list: list[str] = ['00126380']
api_key: str = cast(str, os.environ.get('DART_API_KEY'))

@dag(
    schedule='0 0 1 * *',
    start_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['dart']
)
def raw_dart():
    
    @task.branch()
    def is_new_quarter(data_interval_start: DateTime):
        print('hi')
        if data_interval_start.month not in (1, 4, 7, 10) or data_interval_start.day != 1:
            return None
        return 'begin'
    
    @task()
    def begin(data_interval_start: DateTime):
        business_year: int = data_interval_start.year
        quarter: int = 0
        report_code: str = ''

        if data_interval_start.month == 1:
            quarter = 4
            business_year = data_interval_start.year - 1
            report_code = '11011'
        elif data_interval_start.month == 4:
            quarter = 1
            report_code = '11013'
        elif data_interval_start.month == 7:
            quarter = 2
            report_code = '11012'
        elif data_interval_start.month == 10:
            quarter = 3
            report_code = '11014'

        hook = PostgresHook(postgres_conn_id='postgres_default')
        for company_code in company_code_list:
            query_strings = {
                'crtfc_key': api_key, 
                'corp_code': company_code,
                'bsns_year': business_year,
                'reprt_code': report_code,
            }
            
            print('financial')
            financial_response: Response = request('get', origin + '/api/fnlttSinglAcnt.json', params=query_strings)
            print('dividend')
            dividend_response: Response = request('get', origin + '/api/alotMatter.json', params=query_strings)
            print('request end')

            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(
                'insert into raw_company(id, year, quarter, financial, dividend) values (%s, %s, %s, %s, %s)', 
                (company_code, business_year, quarter, financial_response.text, dividend_response.text)
            )
            connection.commit()
            print('db end')

    is_new_quarter() >> begin()

raw_dart()