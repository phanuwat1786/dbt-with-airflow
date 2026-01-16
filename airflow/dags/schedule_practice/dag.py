import pendulum
from airflow import DAG
from airflow.decorators import task
from custom_timetable import ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable

with DAG (
    dag_id = 'custom_timetable_1',
    start_date = pendulum.parse('2026-01-15',tz = 'UTC'),
    timetable = ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable(),
    catchup= False
):
    @task
    def start():
        pass
    
    @task 
    def end():
        pass

    start() >> end()