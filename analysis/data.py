"""
This encapsulates all classes / methods used to prepare data stored in the database for further analysis (or just show)
"""
from scipy import stats

from persistence.analysis import *


class AnalysisDataSource:

    def __init__(self, db: str, host: str, user: str, password: str):
        self.persistence = AnalysisPersistence(db=db, host=host, user=user, password=password)

    def temperature_daily_chronology(self, sensor_location: str, the_date: datetime = datetime.now()):
        _the_sensor = self.persistence.get_sensor_by_location(sensor_location=sensor_location,
                                                              sensor_type_name=SENSORTYPE_TEMPERATURE)
        return TemperatureDailyChronology(
            self.persistence.temperature_daily_chronology(the_sensor=_the_sensor, the_date=the_date),
            the_date=the_date)


class TemperatureDailyChronology:

    def __init__(self, records: list, the_date: datetime = None):
        if records is None:
            raise ValueError('Attempt to initialize temperature chronology without records')
        if not the_date and len(records) > 0:
            the_date = records[0].timestamp

        self._raw_records = records
        self.the_date = the_date
        self._raw_timeline = None
        self._raw_temperatures = None
        self._ph_timeline = None
        self._ph_temperatures = None
        self._ph_errors_upper = None
        self._ph_errors_lower = None
        self._ph_errors = None
        self._raw_min = None
        self._raw_max = None

    def _init_raw(self):
        self._raw_timeline = list()
        self._raw_temperatures = list()
        self._raw_min = None
        self._raw_max = None
        for record in self._raw_records:
            self._raw_timeline.append(record.timestamp)
            self._raw_temperatures.append(record.temperature)
            if not self._raw_min or self._raw_min.temperature > record.temperature:
                self._raw_min = record
            if not self._raw_max or self._raw_max.temperature < record.temperature:
                self._raw_max = record

    def _init_ph(self):
        self._ph_timeline = [datetime(year=self.the_date.year, month=self.the_date.month, day=self.the_date.day,
                                      hour=h, minute=0, second=0)
                             for h in range(0, self.the_date.hour + 1)]
        if self.the_date.hour == 23:
            self._ph_timeline.append(self.the_date)

        self._ph_temperatures = list()
        self._ph_errors_upper = list()
        self._ph_errors_lower = list()
        self._ph_errors = list()

        for _h in self._ph_timeline:
            # find both the closest reading in the past and in the future
            # together with value from last and next hour from the timemark _h
            closest_down = None
            closest_up = None
            last_hour = list()
            next_hour = list()

            for _r in self._raw_records:
                if _h > _r.timestamp:
                    closest_down = _r
                if _h < _r.timestamp and not closest_up:
                    closest_up = _r
                if 0 < (_h - _r.timestamp).total_seconds() <= 60*60:
                    last_hour.append(_r)
                if 0 >= (_h - _r.timestamp).total_seconds() >= -60*60:
                    next_hour.append(_r)

            # collect observations that will be considered to calculate
            # the best-approximation of temperature at given hour
            # if there is no observation within an hour (in past and towards future), choose the closest reading
            observations = list()
            if len(last_hour) > 0:
                observations.extend(last_hour)
            elif closest_down:
                observations.append(closest_down)
            if len(next_hour) > 0:
                observations.extend(next_hour)
            elif closest_up:
                observations.append(closest_up)

            if len(observations) > 1:
                # for the linear regression choose the first non-empty: last\next hour or closest value
                # x: seconds from time-mark, y: the temperature at given point
                y = [observation.temperature for observation in observations]
                x = [(observation.timestamp - _h).total_seconds() for observation in observations]
                lr_res = stats.linregress(x, y)
                # intercept is the temperature I'm looking for
                self._ph_temperatures.append(float(lr_res.intercept))
            elif len(observations) == 1:
                self._ph_temperatures.append(observations[0].temperature)

            # calculate min and max within the observations and then subtract from 'average' value to get errors
            self._ph_errors_lower.append(abs(self._ph_temperatures[-1] - min(
                [observation.temperature for observation in observations])))
            self._ph_errors_upper.append(abs(self._ph_temperatures[-1] - max(
                [observation.temperature for observation in observations])))

    def get_raw_timeline(self) -> list:
        if not self._raw_timeline:
            self._init_raw()
        return self._raw_timeline

    def get_raw_temperatures(self) -> list:
        if not self._raw_temperatures:
            self._init_raw()
        return self._raw_temperatures

    def get_raw_last(self) -> tuple:
        return self._raw_records[-1].timestamp, self._raw_records[-1].temperature

    def get_perhour_timeline(self) -> list:
        if not self._ph_timeline:
            self._init_ph()
        return self._ph_timeline

    def get_perhour_temperatures(self) -> list:
        if not self._ph_temperatures:
            self._init_ph()
        return self._ph_temperatures

    def get_perhour_last(self) -> tuple:
        if not self._ph_temperatures:
            self._init_ph()

        return self._ph_timeline[-1], self._ph_temperatures[-1]

    def get_min(self) -> tuple:
        if not self._raw_min:
            self._init_raw()
        return self._raw_min.timestamp, self._raw_min.temperature

    def get_max(self) -> tuple:
        if not self._raw_max:
            self._init_raw()
        return self._raw_max.timestamp, self._raw_max.temperature

    def get_lower_errors(self) -> list:
        if not self._ph_errors_lower:
            self._init_ph()
        return self._ph_errors_lower

    def get_upper_errors(self) -> list:
        if not self._ph_errors_upper:
            self._init_ph()
        return self._ph_errors_upper


def analysis_data_source(credentials_file: str = None, config_file: str = None) -> AnalysisDataSource:
    """
    Utilitary method for easy configuration of the persistence. It accesses deployment configuration files in order to
    obtain both location of the database engine and the credentials.
    Note that this method may only be used in "development" or "debug" mode
    If the paths to config files are not provided, the method will eagerly look up for them.
    NOTE: this is almost copy+paste of similar method in persistence.analysis module
    :param credentials_file: path to the file containing credentials (if missing, will be guessed)
    :param config_file: path to file specifying the location of the mariadb (if missing, will be guessed)
    :return: new AnalysisPersistence objects, which encapsulates all methods used by analysis
    """

    if not credentials_file or not config_file:
        _deployment_project_name = 'BHS_Deployment'

        _lookup_paths = ['../', '../../']
        _base_path = None
        for _path in _lookup_paths:
            _content = listdir(_path)
            for dr in _content:
                if path.basename(dr) == _deployment_project_name:
                    _base_path = path.abspath(path.join(_path, dr))
                    break

        if not _base_path:
            raise ValueError('Failed to guess where the credentials and configs are located')

        if not credentials_file:
            credentials_file = path.join(_base_path, 'install', '.credentials')

        if not config_file:
            config_file = path.join(_base_path, 'install', 'common.install.ini')

    section_db = 'DATABASE'
    section_credentials = 'REST-INFO'
    option_db = 'db'
    option_host = 'host'
    option_user = 'user'
    option_password = 'password'

    configs = ConfigParser(allow_no_value=True)
    configs.read([credentials_file, config_file])

    _db = configs.get(section_db, option_db)
    _host = configs.get(section_db, option_host)
    _user = configs.get(section_credentials, option_user)
    _password = configs.get(section_credentials, option_password)

    if not _db or not _host or not _user or not _password:
        raise ValueError('Cant locate all the necessary options')

    return AnalysisDataSource(db=_db, user=_user, password=_password, host=_host)


if __name__ == "__main__":
    db = analysis_data_source()
    t = db.temperature_daily_chronology('External')
    print(t.get_perhour_teperatures())
    print(t.get_perhour_timeline())
    print(t.get_perhour_last())
    print(t.get_raw_teperatures())
    print(t.get_raw_timeline())
    print(t.get_raw_last())
