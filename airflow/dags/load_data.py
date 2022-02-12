from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
# [END default_args]

# [START instantiate_dag]
load_initial_data_dag = DAG(
    '1_load_initial_data',
    default_args=default_args,
    schedule_interval = None,
)

t1 = PostgresOperator(task_id='create_schema',
                      sql="CREATE SCHEMA IF NOT EXISTS dbt_raw_data;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t2 = PostgresOperator(task_id='drop_table_awards',
                      sql="DROP TABLE IF EXISTS awards;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t3 = PostgresOperator(task_id='create_awards',
                      sql="""
                      CREATE TABLE IF NOT EXISTS 
                      dbt_raw_data.awards 
                      (
                          taxid bigint, 
                          date date, 
                          name varchar(100)
                      );
                      """,
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t4 = PostgresOperator(task_id='load_awards',
                      sql="""
                      COPY dbt_raw_data.awards 
                      FROM '/sample_data/raw__awards.csv'
                      DELIMITER ',' CSV HEADER NULL as 'nan';
                      """,
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t5 = PostgresOperator(task_id='drop_table_officers',
                      sql="DROP TABLE IF EXISTS officers;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t6 = PostgresOperator(task_id='create_officers',
                      sql="""
                      CREATE TABLE IF NOT EXISTS 
                      dbt_raw_data.officers 
                      (
                        taxid integer, 
                        full_name varchar(100), 
                        first_name varchar (50), 
                        last_name varchar (50),
                        middle_initial varchar(5), 
                        command varchar (100), 
                        rank varchar(100), 
                        shield_no float, 
                        appt_date date, 
                        recognition_count integer, 
                        arrest_count integer, 
                        ethnicity varchar(100),
                        assignment_date date, 
                        arrests_infr float, 
                        arrests_misd float, 
                        arrests_fel float, 
                        arrests_violation float, 
                        arrests_other float
                      );
                      """,
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t7 = PostgresOperator(task_id='load_officers',
                      sql="""
                      COPY dbt_raw_data.officers 
                      FROM '/sample_data/raw__officers.csv' 
                      DELIMITER ',' CSV HEADER
                      NULL as '';""",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)                     

t8 = PostgresOperator(task_id='drop_table_penalties',
                      sql="DROP TABLE IF EXISTS penalties;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t9 = PostgresOperator(task_id='create_penalties',
                      sql="""
                      CREATE TABLE IF NOT EXISTS dbt_raw_data.penalties (
                        taxid int,
                        date date,
                        disposition varchar(100),
                        command varchar(50),
                        case_no varchar(50),
                        description varchar(500),
                        penalty varchar(500),
                        type varchar(50),
                        recommendation varchar(500) 
                          );""",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t10 = PostgresOperator(task_id='load_penalties',
                      sql="""
                      COPY dbt_raw_data.penalties 
                      FROM '/sample_data/raw__discipline.csv' 
                      DELIMITER ',' CSV HEADER
                      NULL as 'nan';""",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)  

t11 = PostgresOperator(task_id='drop_table_allegations',
                      sql="DROP TABLE IF EXISTS allegations;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t12 = PostgresOperator(task_id='create_allegations',
                      sql="""
                      CREATE TABLE IF NOT EXISTS dbt_raw_data.allegations (
                          officer_id int,
                          command varchar(20),
                          last_name varchar(50),
                          first_name varchar(50),
                          rank varchar(50),
                          shield_no int,
                          active varchar(20),
                          complaint_id int,
                          complaint_date date,
                          fado_type varchar(50),
                          allegation varchar(100),
                          board_disp varchar(100),
                          nypd_disp varchar(100),
                          penalty_desc varchar(200)
                      );
                      """,
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t13 = PostgresOperator(task_id='load_allegations',
                      sql="""
                      COPY dbt_raw_data.allegations 
                      FROM '/sample_data/raw__complaint_records.csv' 
                      DELIMITER ',' CSV HEADER
                      NULL as '';""",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)  


# t14 = PostgresOperator(task_id='drop_table_order_products__prior',
#                       sql="DROP TABLE IF EXISTS order_products__prior;",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)

# t15 = PostgresOperator(task_id='create_order_products__prior',
#                       sql="create table if not exists dbt_raw_data.order_products__prior(order_id integer, product_id integer, add_to_cart_order integer, reordered integer);",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)

# t16 = PostgresOperator(task_id='load_order_products__prior',
#                       sql="""
#                       COPY dbt_raw_data.order_products__prior 
#                       FROM '/sample_data/order_products__prior.csv' 
#                       DELIMITER ',' CSV HEADER;""",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)   

# t17 = PostgresOperator(task_id='drop_table_order_products__train',
#                       sql="DROP TABLE IF EXISTS order_products__train;",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)

# t18 = PostgresOperator(task_id='create_order_products__train',
#                       sql="create table if not exists dbt_raw_data.order_products__train(order_id integer, product_id integer, add_to_cart_order integer, reordered integer);",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)

# t19 = PostgresOperator(task_id='load_order_products__train',
#                       sql="""
#                       COPY dbt_raw_data.order_products__train 
#                       FROM '/sample_data/order_products__train.csv' 
#                       DELIMITER ',' CSV HEADER;""",
#                       postgres_conn_id='dbt_postgres_instance_raw_data',
#                       autocommit=True,
#                       database="dbtdb",
#                       dag=load_initial_data_dag)       

t1 >> t2 >> t3 >> t4
t1 >> t5 >> t6 >> t7
t1 >> t8 >> t9 >> t10
t1 >> t11 >> t12 >> t13
# t1 >> t14 >> t15 >> t16
# t1 >> t17 >> t18 >> t19