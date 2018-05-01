import logging
import pandas
import requests
from gpxpy.geo import haversine_distance
from io import StringIO


class QuakeQuery(object):
    """
        This class queries earthquake data from INGV and returns
        a pandas DataFrame with the result.

        Bart Pelssers, 2017-01-22
    """
    m_per_deg = 40075000 / 360  # earth circumference in meters / 360 degrees

    # Some big earthquakes near LNGS
    quakes = {'2009': ('2009-04-05T00:00:00', '2009-04-12T00:00:00'),
              'august_2016': ('2016-08-23T00:00:00', '2016-09-07T00:00:00'),
              'october_2016': ('2016-10-01T00:00:00', '2016-10-30T00:00:00'),
              'january_2017': ('2017-01-18T00:00:00', '2017-01-25T00:00:00')
              }

    def __init__(self, latitude=42.42209, longitude=13.51519,
                 radius=80000, loglevel=logging.INFO):
        """
            Initialize QuakeQuery.
            Defaults to 80km radius around LNGS surface lab.
        """
        self.logger = logging.getLogger("QuakeQuery")
        self.logger.setLevel(loglevel)

        self.latitude = latitude
        self.longitude = longitude
        self.radius = radius

        self.calc_bounding_box()

        self.logger.info("Initialized at %.2f lat, %.2f lon with %.0fm radius"
                         % (latitude, longitude, radius))

    def calc_bounding_box(self):
        delta = self.radius / self.m_per_deg
        self.lat_range = (self.latitude - delta, self.latitude + delta)
        delta /= pandas.np.cos(pandas.np.pi * self.latitude /180)
        self.lon_range = (self.longitude - delta, self.longitude + delta)

    def query(self, start_date, end_date='now', min_mag=2, max_mag=10):
        """
            Query INGV for quakes in a given time and magnitude range.
        """
        if end_date == 'now':
            now_utc = pandas.to_datetime('now', utc=True)
            end_date = now_utc.strftime('%Y-%m-%dT%H:%M:%S')

        # Construct payload for the request
        payload = {'starttime': start_date,
                   'endtime': end_date,
                   'minmag': min_mag,
                   'maxmag': max_mag,
                   'mindepth': -10,
                   'maxdepth': 1000,
                   'minlat': "%.3f" % self.lat_range[0],
                   'maxlat': "%.3f" % self.lat_range[1],
                   'minlon': "%.3f" % self.lon_range[0],
                   'maxlon': "%.3f" % self.lon_range[1],
                   'minversion': 100,
                   'orderby': 'time-asc',
                   'format': 'text',
                   'limit': 10000,
                   }

        url = "http://webservices.ingv.it/fdsnws/event/1/query"

        try:
            r = requests.get(url, params=payload)
        except Exception as e:
            self.logger.error(e)

        if not r.status_code == 200:
            self.logger.error("Something went wrong:\n", r.text)
        else:
            text_data = StringIO(r.text)
            self.data = pandas.read_csv(text_data, sep='|')
            if len(self.data) >= 10000:
                self.logger.warning("Query limit reached! Select a smaller "
                                    "time/radius/magnitude range.")

            # Add datetime objects instead of strings
            self.data['DateTime'] = pandas.to_datetime(self.data['Time'])
            # Add a unix (seconds) timestamp
            self.data['Timestamp'] = self.data['DateTime'].astype(int) // 10**9

            # Clean up the dataframe
            self.data.drop(['Author',
                            'Time',
                            'Catalog',
                            'Contributor',
                            'ContributorID',
                            'MagAuthor'], axis=1, inplace=True)

            # Calculate distance of quake in meters
            def add_distance(row):
                return haversine_distance(row['Latitude'],
                                          row['Longitude'],
                                          self.latitude,
                                          self.longitude)

            # Add distance column to data
            self.data['Distance'] = self.data.apply(add_distance, axis=1)

            # Cut on radius
            self.data = self.data[self.data['Distance'] < self.radius]

            self.logger.info("Queried %d events", len(self.data))

            return self.data

    def query_predefined(self, name, min_mag=2, max_mag=10):
        """
            Query some predefined earthquake times.
        """
        if name in self.quakes:
            start, end = self.quakes[name]
            return self.query(start, end, min_mag, max_mag)
        else:
            self.logger.error("%s not in list of predefined earthquakes"
                              % name)
