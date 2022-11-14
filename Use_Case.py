import os
import tabulate
from sql_server import DBHelper

"""
Samples of use cases.
"""


def get_available_station(db) -> None:
    """
    Get current available station and save as view.
    :param db: Lib. Mysql Helper.
    :return: None.
    """
    sql = "SELECT status, COUNT(*) as Station_number FROM station GROUP BY status " \
          "union all " \
          "select 'total' as status, count(sid) as Station_number from station"
    out = db.fetch_all(sql)
    header = out[0].keys()
    rows = [x.values() for x in out]
    table = tabulate.tabulate(rows, header)
    print(table)
    # save view
    sql = "CREATE OR REPLACE VIEW available_station AS " + sql
    db.exec(sql)


def get_station_per_region(db, region) -> None:
    """
    Get the station info. grouped by region.
    :param db: Lib. Mysql Helper.
    :param region: Str 'city' or 'state'.
    :return: None.
    """
    sql = "select {}, avg(latitude) as Latitude, avg(longitude) as Longitude, " \
          "COUNT(status = 'AVAILABLE' OR NULL) as Availabel_station," \
          "COUNT(status = 'CHARGING' OR NULL) as Charging_station, count(*) as Total_station, " \
          "COUNT(status = 'CHARGING' OR NULL)*1.0/count(*) as Cur_utilization from station " \
          "group by {} order by count(*) desc".format(region, region)
    out = db.fetch_all(sql)
    header = out[0].keys()
    rows = [x.values() for x in out]
    table = tabulate.tabulate(rows, header)
    print(table)
    # save view
    sql = "CREATE OR REPLACE VIEW station_per_{} AS ".format(region) + sql
    db.exec(sql)


def get_average_utilization(db, region, update_view, day=1) -> None:
    """
    Get average utilization of plug in specified time grouped by region.
    :param db: Lib. Mysql Helper.
    :param region: Str 'city' or 'state'.
    :param update_view: Bool whether refresh view.
    :param day: Int the day range for data processing.
    :return: None.
    """
    # create or update plug usage view
    create_view = "CREATE OR REPLACE VIEW EV_plug_usage (s_id, Available_plug, Charging_plug, Total_plug) AS " \
                  "(select t.s_id as s_id, sum(t.Available_plug) as Available_plug, sum(t.Charging_plug) as Charging_plug," \
                  "sum(t.Total_plug) as Total_plug from (" \
                  "select pid, max(ts), max(station_id) as s_id, round(count(plug_status = 'AVAILABLE' or null)/count(*)) as Available_plug," \
                  "round(count(plug_status = 'CHARGING' or null)/count(*)) as Charging_plug, " \
                  "round(count(plug_status)/count(*)) as Total_plug " \
                  "from plug WHERE DATE_SUB(CURDATE(), INTERVAL {} DAY) <= DATE(ts) group by pid) t group by t.s_id)".format(day)
    if update_view:
        db.exec(create_view)

    sql = "select {}, avg(latitude) as Latitude, avg(longitude) as Longitude, sum(ev_plug_usage.Available_plug) as Avg_available_plug, " \
          "sum(ev_plug_usage.Charging_plug) as Avg_charging_plug, sum(ev_plug_usage.Total_plug) as Avg_total_plug, " \
          "sum(ev_plug_usage.Charging_plug)/sum(ev_plug_usage.Total_plug) as Ave_utilization from station left join " \
          "ev_plug_usage on station.sid=ev_plug_usage.s_id " \
          "group by {} " \
          "order by Avg_total_plug DESC ".format(region, region)
    out = db.fetch_all(sql)
    header = out[0].keys()
    rows = [x.values() for x in out]
    table = tabulate.tabulate(rows, header)
    print(table)
    # save view
    sql = "CREATE OR REPLACE VIEW average_utilization_per_{} AS ".format(region) + sql
    db.exec(sql)


if __name__ == '__main__':
    db = DBHelper()

    function = "0. get the number of available EV stations.\n" \
               "1. get EV stations info. per city.\n" \
               "2. get EV stations info. per state.\n" \
               "3. get average utilization of EV stations per city in specified time.\n" \
               "4. get average utilization of EV stations per state in specified time.\n" \
               "5. exit.\n"\
               ":"

    while True:
        x = input(function)

        if x == '0':
            get_available_station(db)
            if input() == '':
                os.system("cls")
                continue
        elif x == '1':
            get_station_per_region(db, 'city')
            if input() == '':
                os.system("cls")
                continue
        elif x == '2':
            get_station_per_region(db, 'state')
            if input() == '':
                os.system("cls")
                continue
        elif x == '3':
            day = input("load the data from last day (default 1): ")
            update_view = True if day else False
            get_average_utilization(db, day=day, update_view=update_view, region='city')
            if input() == '':
                os.system("cls")
                continue
        elif x == '4':
            day = input("load the data from last day (default 1): ")
            update_view = True if day else False
            get_average_utilization(db, day=day, update_view=update_view, region='state')
            if input() == '':
                os.system("cls")
                continue
        elif x == '5':
            break
