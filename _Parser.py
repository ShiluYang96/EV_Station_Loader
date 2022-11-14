import time
import requests
import reverse_geocoder as rg

from sql_server import DBHelper


class Parser:
    """
    Read process & save API data.
    """
    def __init__(self,
                 db,
                 rg,
                 time_out: int = 100,
                 cycle_time: int = 5,
                 limit: int = 100,
                 resume: int = 0):
        """
        Initialize a API_Reader object.
        :param db: class sql databank helper.
        :param rg: lib. mapping  Latitude & Longitude to city name.
        :param time_out: out time for request.
        :param cycle_time: time interval between two requests.
        :param limit: the number of data context in one request.
        :param resume: resume query from the specified page.
        """
        self.db = db
        self.rg = rg
        self.time_out = time_out
        self.cycle_time = cycle_time
        self.limit = limit
        self.resume = resume
        self.total_station_num = self.get_total_station_num()  # 18059
        self.pages = int(self.total_station_num / self.limit) + 2

    def get_total_station_num(self) -> int:
        """
        Get the number of stations.
        :return: int station number.
        """
        url = "https://api.plugsurfing.com/mfund/stations"
        response = requests.get(url, timeout=self.time_out)
        return int(response.headers['X-Total-Count'])

    def save_data(self, table, data) -> None:
        """
        Save API data in mysql DB.
        """
        if table == 'station':
            keys = ("sid", "country_code", "city", "state", "utilization")
        elif table == 'connector':
            keys = ("cid", "speed", "power_type", "station_id")
        else:
            raise ValueError("Unknown table name.")
        try:
            self.db.insert(table, keys, data)
        except:  # duplicate
            pass

    def read_api(self) -> None:
        """
        Query the API information.
        """
        for page in range(1, self.pages):
            if page < self.resume:
                continue
            # request until get api data
            while True:
                url = "https://api.plugsurfing.com/mfund/stations?page={}&limit={}".format(page, self.limit)
                print("Downloading", url)
                response = requests.get(url, timeout=self.time_out)

                if response.status_code == 200:
                    start = time.time()
                    response_data = response.json()
                    processed_data = self.process_data(response_data)
                    # print(time.time() - start)
                    self.save_data(table="station", data=processed_data[0])
                    self.save_data(table="connector", data=processed_data[1])

                    elapsed_time = time.time() - start
                    # Pause program if time
                    # print(elapsed_time)
                    if elapsed_time < self.cycle_time:
                        time.sleep(self.cycle_time - elapsed_time)
                    break
                else:
                    time.sleep(self.cycle_time)
                    print(response)

    def process_data(self, reponse_data) -> list:
        """
        Process the raw data.
        :return: list of station data and connector data.
        """
        station_data = []
        connector_data = []
        for data in reponse_data:
            # process station data
            station_id = data['id']
            utilization = data['utilization']
            # Latitude & Longitude input
            Latitude = data['latitude']
            Longitude = data['longitude']
            coordinates = (Latitude, Longitude)

            address = self.rg.search(coordinates, mode=1)[0]
            city = address['name']
            state = address['admin1']
            country = address['cc']

            station_data.append((station_id, country, city, state, utilization))

            # process connector data
            for connector in data['connectors']:
                connector_id = connector['id']
                speed = connector['speed']
                powerType = connector['powerType']
                connector_data.append((connector_id, speed, powerType, station_id))

        return [station_data, connector_data]


if __name__ == '__main__':
    # initialize Nominatim API
    db = DBHelper()
    try:
        parser = Parser(db, rg)
        parser.read_api()
    except KeyboardInterrupt:
        pass
