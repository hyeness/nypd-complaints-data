from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
import pandas as pd
import urllib

def download():
    write_dict = get_data()
    output_csv(write_dict)

def get_data():
    awards = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/awards.csv",
        parse_dates=["date"],
    )
    discipline = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/discipline.csv",
        parse_dates=["date"],
    )
    records_ryan = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/ccrb-complaint-records/main/records.csv",
        parse_dates=["complaint_date"],
    )
    officers__ccrb = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/ccrb-complaint-records/main/officers.csv"
    )
    officers = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/officers.csv",
        parse_dates=["appt_date", "assignment_date"],
    )
    complaints = pd.read_csv(
        "https://raw.githubusercontent.com/ryanwatkins/ccrb-complaint-records/main/complaints.csv",
        parse_dates=["complaint_date"],
    )

    lawsuits_data_urls = {
        'lawsuits_2017_2021': 'https://www1.nyc.gov/site/law/public-resources/nyc-administrative-code-7-114.page#:~:text=Download%20NYPD%20Alleged%20Misconduct%20Matters%20commenced%20in%20CY%202017%2D2021',
        'lawsuits_2013_2017': 'https://www1.nyc.gov/site/law/public-resources/nyc-administrative-code-7-114.page#:~:text=Download%20NYPD%20Alleged%20Misconduct%20Matters%20commenced%20in%20CY%202013%2D2017'
    }

    lawsuits_dfs = []
    for fn, url in lawsuits_data_urls.items():
        print(fn)
        fp = f"/app/sample_data/{fn}.xls"
        urllib.request.urlretrieve(url, fp)
        lawsuits_dfs.append(pd.read_excel(fp))
    lawsuits_df = pd.concat(lawsuits_dfs).drop_duplicates(subset=['Tax #', 'Docket/\nIndex#'])

    write_dict = {
        "raw__awards": awards,
        "raw__discipline": discipline,
        "raw__complaint_records": records_ryan,
        "raw__officers": officers,
        "raw__officers__ccrb": officers__ccrb,
        "raw__complaints": complaints,
        "raw__lawsuits": lawsuits_df
    }
    return write_dict

def output_csv(write_dict):
    for fn, df in write_dict.items():
        fp = f"/app/sample_data/{fn}.csv"
        print(f"Writing file to {fp}")
        df.to_csv(f"{fn}.csv", index=False)

with DAG(
    '0_download_data',
    default_args=default_args,
    schedule_interval=None 
) as dag:
    t0 = PythonOperator(
        task_id='download_data', 
        python_callable=download, 
        dag=dag
        )

# if __name__ =='__main__':
#     download()


