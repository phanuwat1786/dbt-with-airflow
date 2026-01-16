from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable,DataInterval, DagRunInfo

from pendulum import now,yesterday,DateTime,Time,UTC

from airflow.timetables.base import TimeRestriction

class ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable(Timetable):
    def infer_manual_data_interval(self,run_after: DateTime) -> DataInterval:

        if run_after <= DateTime.combine(date= now().date(),time = Time(minute=30)):
            start = DateTime.combine(date=yesterday().date(),time = Time(hour=17))
            end = DateTime.combine(date = now().date(),time=Time(minute=30))

        elif run_after <= DateTime.combine(date = now().date(),time = Time(hour=17)):
            start = DateTime.combine(date = now().date(),time = Time(minute=30))
            end = DateTime.combine(date = now().date(), time= Time(hour=17))
        
        else :
            start = DateTime.combine(date = now().date(), time = Time(hour=17))
            end = DateTime.combine(date = now().add(days=1).date(),time = Time(minute=30))

        return DataInterval(start= start.replace(tzinfo=UTC),end = end.replace(tzinfo=UTC))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    )-> DagRunInfo | None:
        if last_automated_data_interval is not None:

            prev_end = last_automated_data_interval.end
            next_start = prev_end
            
            if next_start.time() == Time(minute=30):
                next_end = DateTime.combine(date = next_start.date(),time = Time(hour=17))
            else :
                next_end = DateTime.combine(date = next_start.date().add(days =1),time = Time(minute = 30))
        
        elif (earliest := restriction.earliest) is None:
            return None

        elif not restriction.catchup:

            start_date = max(earliest.date(),now().date())

            if (current_time := now().time()) <= Time(minute=30):
                next_start = DateTime.combine(date = start_date.add(days= -1), time = Time(hour = 17))
                next_end = DateTime.combine(date = start_date,time = Time(minute=30))

            elif current_time <= Time(hour=17):
                next_start = DateTime.combine(date = start_date, time = Time(minute= 30))
                next_end = DateTime.combine(date = start_date,time = Time(hour=17))

            else:
                next_start = DateTime.combine(date = start_date, time = Time(hour= 17))
                next_end = DateTime.combine(date = start_date.add(days=1),time = Time(minute=0))

        elif earliest.time() <= Time(minute=30) : 
            next_start = DateTime.combine(date = earliest.date().add(days= -1), time = Time(hour = 17))
            next_end = DateTime.combine(date = earliest.date(),time = Time(minute=30))

        elif earliest.time() <= Time(hour = 17) :
            next_start = DateTime.combine(date = earliest.date(), time = Time(minute= 30))
            next_end = DateTime.combine(date = earliest.date(),time = Time(hour=17))

        else:
            next_start = DateTime.combine(date = earliest.date(), time = Time(hour= 17))
            next_end = DateTime.combine(date = earliest.date().add(days=1),time = Time(minute=30))
        
        if restriction.latest is not None and next_start > restriction.latest:
            return None

        return DagRunInfo.interval(start = next_start.replace(tzinfo=UTC), end = next_end.replace(tzinfo=UTC))

class ThirtyMinuteAfterMidnightAndFivePMDaily(AirflowPlugin):
    name = "thirty_minute_after_midnight_and_five_pm_daily_plugin"
    timetables = [ThirtyMinuteAfterMidnightAndFivePMDailyTimeTable]