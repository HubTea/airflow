import json
from enum import Enum

from pendulum import DateTime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

company_code_list: list[str] = ['00126380']
return_on_equity_threshod: float = 1
debt_equity_ratio_threshod: float = 3
highest_dividend_yield: float = 3.0

class StockType(Enum):
    COMMON = 'common'
    PREFERRED = 'preferred'

class Quarter:
    year: int
    quarter: int

    def __init__(self, year, quarter):
        self.year = year
        self.quarter = quarter

class Account:
    account_nm: str #항목 이름
    thstrm_amount: str #당기 금액
    frmtrm_amount: str #전기 금액

class Financial:
    list: list[Account]

class DividendEntry:
    se: str #항목 이름
    thstrm: str #당기 시가 배당률
    frmtrm: str #전기 시가 배당률
    stock_knd: str # 배당 종류
    corp_name: str # 회사 이름

class Dividend:
    list: list[DividendEntry]

class Report:
    comany_id: str
    year: int
    quarter: int
    financial: Financial
    dividend: Dividend

    def __init__(self, company_id: str, year: int, quarter: int, financial: Financial, dividend: Dividend):
        self.comany_id = company_id,
        self.year = year,
        self.quarter = quarter,
        self.financial = financial,
        self.dividend = dividend,

def get_previous_quarter(datetime: DateTime) -> Quarter:
    if datetime.month in range(1, 4):
        return Quarter(datetime.year - 1, 4)
    elif datetime.month in range(4, 7):
        return Quarter(datetime.year, 1)
    elif datetime.month in range(7, 10):
        return Quarter(datetime.year, 2)
    return Quarter(datetime.year, 3)
    

@dag(
    tags=['dart']
)
def process():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    @task()
    def extract(data_interval_start: DateTime):
        '''가장 최근 분기의 회사 정보를 추출'''
        quarter: Quarter = get_previous_quarter(data_interval_start)

        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(
            'select * from raw_company where year = %s and quarter = %s', 
            (quarter.year, quarter.quarter)
        )
        record_list = cursor.fetchmany()
        connection.commit()
        return map(lambda x: Report(x[0], x[1], x[2], json.load(x[3]), json.load(x[4])), record_list)

    @task()
    def transformAndLoad(report_list: list[Report]):
        for report in report_list:
            debt_equity_ratio: float
            return_on_equity: float
            highest_dividend_yield: float

            name: str
            liabilities: int
            equity: int
            assets: int
            revenue: int
            operating_income: int
            net_income: int

            highest_dividend_type: StockType
            common_stock_yield: float
            preferred_stock_yield: float

            newly_crossed_debt_equity_ratio_threshold: bool
            newly_crossed_return_on_equity_threshold: bool
            newly_crossed_highest_dividend_yield: bool

            for entry in report.financial.list:
                if entry.account_nm == '부채총계':
                    liabilities = int(entry.thstrm_amount)
                elif entry.account_nm == '자본총계':
                    equity = int(entry.thstrm_amount)
                elif entry.account_nm == '자산총계':
                    assets = int(entry.thstrm_amount)
                elif entry.account_nm == '매출액':
                    revenue = int(entry.thstrm_amount)
                elif entry.account_nm == '영업이익':
                    operating_income = int(entry.thstrm_amount)
                elif entry.account_nm == '당기순이익':
                    net_income = int(entry.thstrm_amount)

            debt_equity_ratio = liabilities / equity
            return_on_equity = net_income / equity
            name = report.dividend.list[0].corp_name
            
            for entry in report.dividend.list:
                if entry.se != '현금배당수익률(%)':
                    continue
                
                if entry.stock_knd == '보통주':
                    common_stock_yield = float(entry.thstrm)
                    previous_common_stock_yield = float(entry.frmtrm)

                    if common_stock_yield > previous_common_stock_yield:
                        newly_crossed_highest_dividend_yield = True

                elif entry.stock_knd == '우선주':
                    preferred_stock_yield = float(entry.thstrm)
                    previous_preferred_stock_yield = float(entry.frmtrm)

                    if preferred_stock_yield > previous_preferred_stock_yield:
                        newly_crossed_highest_dividend_yield = True

            if common_stock_yield > preferred_stock_yield:
                highest_dividend_type = 'common'
                highest_dividend_yield = common_stock_yield
            else:
                highest_dividend_type = 'preferred'
                highest_dividend_yield = preferred_stock_yield
            
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(
                '''insert into company(
                    id, 
                    debt_equity_ratio, 
                    return_on_equity, 
                    highest_dividend_yield,
                    name,
                    liabilities,
                    equity,
                    assets,
                    revenue,
                    operating_income,
                    net_income,
                    highest_dividend_type, 
                    common_stock_yield,
                    preferred_stock_yield,
                    newly_crossed_debt_equity_ratio_threshold,
                    newly_crossed_return_on_equity_threshold,
                    newly_crossed_highest_dividend_yield
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing''', 
                (
                    report.comany_id,
                    debt_equity_ratio, 
                    return_on_equity, 
                    highest_dividend_yield,
                    name,
                    liabilities,
                    equity,
                    assets,
                    revenue,
                    operating_income,
                    net_income,
                    highest_dividend_type, 
                    common_stock_yield,
                    preferred_stock_yield,
                    newly_crossed_debt_equity_ratio_threshold,
                    newly_crossed_return_on_equity_threshold,
                    newly_crossed_highest_dividend_yield,
                )
            )
            connection.commit()

    data = extract()
    transformAndLoad(data)

process()