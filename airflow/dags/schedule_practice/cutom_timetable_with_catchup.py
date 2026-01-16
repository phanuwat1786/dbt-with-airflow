import pendulum
from airflow import DAG
from airflow.decorators import task
from custom_timetable import ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable

with DAG (
    dag_id = 'custom_timetable_2',
    start_date = pendulum.parse('2026-01-14',tz = 'Asia/Bangkok'),
    timetable = ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable(),
    catchup= True
):
    @task 
    def start():
        pass 
    
    @task 
    def end():
        pass 

    start() >> end()