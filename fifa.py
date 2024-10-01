from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from fifa.extract.extract_club import club_extract
from fifa.extract.extract_league import league_extract
from fifa.extract.extract_nations import nation_extract
from fifa.extract.extract_players import players_extract
from fifa.bronze.process_bronze import bronze_step
from fifa.silver.club_process import silver_step_club
from fifa.silver.nations_process import silver_step_nation
from fifa.silver.league_process import silver_step_league
from fifa.silver.players_process import silver_step_palyers
from fifa.gold.gold_nation import gold_nation
from fifa.gold.gold_league import gold_league
from fifa.gold.gold_clubs import gold_clubs
from fifa.gold.gold_player import gold_players
from fifa.gold.gold_union import union_gold

# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "felipe.pegoraro",
    'email': ['felipepegoraro93@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    "start_date": datetime(2024, 6, 24, tzinfo=local_tz),
}

dag = DAG(
    "fifa",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False
)

# EXTRACT STEP

club = PythonOperator(
    task_id='extract_club',
    python_callable=club_extract,
    dag=dag
)

league = PythonOperator(
    task_id='extract_league',
    python_callable=league_extract,
    dag=dag
)

nation = PythonOperator(
    task_id='extract_nation',
    python_callable=nation_extract,
    dag=dag
)

players = PythonOperator(
    task_id='extract_players',
    python_callable=players_extract,
    dag=dag
)

# BRONZE STEP

bronze = PythonOperator(
    task_id='bronze_transform',
    python_callable=bronze_step,
    dag=dag
)

# SILVER STEP

silver_club = PythonOperator(
    task_id='club_silver_step',
    python_callable=silver_step_club,
    dag=dag
)

silver_nation = PythonOperator(
    task_id='nation_silver_step',
    python_callable=silver_step_nation,
    dag=dag
)

silver_league = PythonOperator(
    task_id='league_silver_step',
    python_callable=silver_step_league,
    dag=dag
)

silver_players = PythonOperator(
    task_id='players_silver_step',
    python_callable=silver_step_palyers,
    dag=dag
)

# GOLD STEP

players_f = PythonOperator(
    task_id='players_gold_step',
    python_callable=gold_players,
    dag=dag
)

league_f = PythonOperator(
    task_id='league_gold_step',
    python_callable=gold_league,
    dag=dag
)

club_f = PythonOperator(
    task_id='club_gold_step',
    python_callable=gold_clubs,
    dag=dag
)

nation_f = PythonOperator(
    task_id='nation_gold_step',
    python_callable=gold_nation,
    dag=dag
)

union = PythonOperator(
    task_id='union_table',
    python_callable=union_gold,
    dag=dag
)

[nation, league, club, players] >> bronze
bronze >> [silver_nation, silver_league, silver_club, silver_players]

silver_nation >> nation_f
silver_league >> league_f
silver_club >> club_f
silver_players >> players_f

[nation_f, league_f, club_f, players_f] >> union