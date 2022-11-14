import time
import requests
import reverse_geocoder as rg

from sql_server import DBHelper

# header for API request
headers = {
    'Accept': 'application/json',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Origin': 'https://api.placetoplug.com',
}

# json data for API query
json_data = {
    'query': 'query Query ($query: FindChargingZone!) {\n  findChargingZones(query: $query) {\n    chargingZones {\n      id\n      name\n      description\n      status\n      coordinates\n      vehicleTypes\n      isPtP\n      isPublic\n      reviews {\n        reviews {\n          id\n          comment\n        }\n        totalCount\n      }\n      stations {\n        id\n        name\n        status\n        services {\n          id\n          name\n          status\n          plugs {\n            id\n            status\n            name\n            current\n            power\n            plugFormat\n          }\n        }\n      }\n    }\n    totalCount\n\t\thasNextPage\n  }\n}\n',
    'variables': {
        'query': {
            "page": 1,
            "limit": 50,
            'filters': {
                'bounds': {
                    "maxLat": 55.0,
                    "maxLon": 15.0,
                    "minLat": 47.0,
                    "minLon": 6.0
                },
            },
        },
    },
}


class API_Reader:
    """
    Read process & save API data.
    """
    def __init__(self,
                 db,
                 rg,
                 time_out: int = 100,
                 cycle_time: int = 1,
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
        self.resume = resume
        self.headers = headers
        self.json_data = json_data
        self.total_station_num = self.get_total_station_num()
        self.pages = int(self.total_station_num / self.json_data['variables']['query']["limit"]) + 2

    def get_total_station_num(self) -> int:
        """
        Get the number of stations.
        :return: int station number.
        """
        response = requests.post('https://api.placetoplug.com/go/graphql', headers=self.headers, json=self.json_data)
        reponse_data = response.json()['data']['findChargingZones']
        time.sleep(self.cycle_time)
        return reponse_data["totalCount"]

    def save_data(self, table, data) -> None:
        """
        Save API data in mysql DB.
        :param table: Str name of table.
        :param data: List data values to be saved.
        :return: None.
        """
        if table == 'station':
            keys = ("sid", "country", "city", "state", "latitude", "longitude", "status")
        elif table == 'plug':
            keys = ("pid", "plug_status", "current", "power", 'format', 'station_id')
        else:
            raise ValueError("Unknown table name.")
        try:
            self.db.insert(table, keys, data)
        except:  # duplicated
            pass

    def read_api(self) -> None:
        """
        Query the API information.
        :return: None.
        """
        for page in range(1, self.pages):
            # whether resume from the specified page
            if page < self.resume:
                continue
            self.json_data['variables']['query']["page"] = page
            # request until get api data
            while True:
                url = 'https://api.placetoplug.com/go/graphql'
                print("Downloading", url, " page: ", page)
                response = requests.post(url, headers=self.headers, json=self.json_data)

                if response.status_code == 200:
                    start = time.time()
                    reponse_data = response.json()['data']['findChargingZones']
                    chargingZones = reponse_data["chargingZones"]
                    processed_data = self.process_data(chargingZones)
                    self.save_data(table="station", data=processed_data[0])
                    self.save_data(table="plug", data=processed_data[1])

                    elapsed_time = time.time() - start
                    # Pause program if time
                    if elapsed_time < self.cycle_time:
                        time.sleep(self.cycle_time - elapsed_time)
                    break
                else:
                    time.sleep(self.cycle_time)
                    print(response)

    def process_data(self, chargingZones) -> list:
        """
        Process the raw data.
        :param chargingZones: Dict input data.
        :return: list of station data and connector data.
        """
        station_data = []
        plug_data = []
        for charging_zone in chargingZones:
            # process station data
            coordinate = charging_zone["coordinates"]
            coordinates = (coordinate[::-1])
            latitude = coordinate[1]
            longitude = coordinate[0]
            address = self.rg.search(coordinates, mode=1)[0]

            city = address['name']
            state = address['admin1']
            country = address['cc']
            if country != 'DE':
                continue

            for station in charging_zone['stations']:
                station_id = station["id"]
                status = station['status']
                station_data.append((station_id, country, city, state, latitude, longitude, status))

                # process plug data
                for services in station["services"]:
                    service_id = services["id"]
                    plug_status = services["status"]
                    plug = services["plugs"][0]
                    current = plug["current"]
                    power = plug["power"]
                    plug_format = plug["plugFormat"]
                    plug_data.append((service_id, plug_status, current, power,plug_format, station_id))

        return [station_data, plug_data]


if __name__ == '__main__':
    # initialize Nominatim API
    db = DBHelper()
    parser = API_Reader(db, rg)
    # refresh 2 times one day
    # station: storage only the data of current day
    # plug: storage max. 30 day data
    # drop data 30 days before --> implement later
    try:
        while True:
            parser.read_api()
            print("Completed! The program has terminated.")
            time.sleep(3600*6)
    except KeyboardInterrupt:
        pass

