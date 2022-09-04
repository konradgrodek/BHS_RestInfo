"""
This encapsulates all classes / methods used to prepare data stored in the database for further analysis (or just show)
"""
from scipy import stats

from persistence.analysis import *
from core.util import SunsetCalculator


class AnalysisDataSource:

    def __init__(self, db: str, host: str, user: str, password: str):
        self.persistence = AnalysisPersistence(db=db, host=host, user=user, password=password)

    def temperature_daily_chronology(self, sensor_location: str, the_date: datetime = datetime.now()):
        _the_sensor = self.persistence.get_sensor_by_location(sensor_location=sensor_location,
                                                              sensor_type_name=SENSORTYPE_TEMPERATURE)
        return TemperatureDailyChronology(
            the_sensor=_the_sensor,
            records=self.persistence.temperature_daily_chronology(the_sensor=_the_sensor, the_date=the_date),
            the_date=the_date)

    def rain_observations(self, the_date: datetime, hours_in_past: int):
        return RainLastHours(
            records=self.persistence.rain_observations_per_last_hours(the_date=the_date, hours_count=hours_in_past),
            the_date=the_date,
            hours_in_the_past=hours_in_past
        )


class TemperatureDailyChronology:

    def __init__(self, the_sensor: Sensor, records: list, the_date: datetime = None):
        if records is None:
            raise ValueError('Attempt to initialize temperature chronology without records')
        if the_sensor is None:
            raise ValueError('Attempt to initialize temperature chronology without sensor')
        if not the_date and len(records) > 0:
            the_date = records[0].timestamp

        self.the_sensor = the_sensor
        self._raw_records = records
        self.the_date = the_date
        self._raw_timeline = None
        self._raw_temperatures = None
        self._ph_timeline = None
        self._ph_temperatures = None
        self._ph_errors_upper = None
        self._ph_errors_lower = None
        self._ph_errors = None
        self._ph_min_temperatures = None
        self._ph_max_temperatures = None
        self._raw_min_daily = None
        self._raw_max_daily = None
        self._raw_avg_daily = None
        self.sun_set_rise = SunsetCalculator(dt=the_date)
        self._raw_min_day = None
        self._raw_max_day = None
        self._raw_avg_day = None
        self._raw_min_night = None
        self._raw_max_night = None
        self._raw_avg_night = None
        _now = datetime.now()
        self._is_for_today = self.the_date.year == _now.year \
            and self.the_date.month == _now.month \
            and self.the_date.day == _now.day

    def is_valid(self):
        return self._raw_records is not None and len(self._raw_records) > 0

    def has_day_data(self):
        return self.is_valid() and self.get_min_day()[0] is not None

    def has_night_data(self):
        return self.is_valid() and self.get_min_night()[0] is not None

    def _init_raw(self):
        self._raw_timeline = list()
        self._raw_temperatures = list()
        self._raw_min_daily = None
        self._raw_max_daily = None
        self._raw_min_day = None
        self._raw_max_day = None
        self._raw_min_night = None
        self._raw_max_night = None
        _day = list()
        _night = list()
        for record in self._raw_records:
            self._raw_timeline.append(record.timestamp)
            self._raw_temperatures.append(record.temperature)
            if not self._raw_min_daily or self._raw_min_daily.temperature > record.temperature:
                self._raw_min_daily = record
            if not self._raw_max_daily or self._raw_max_daily.temperature < record.temperature:
                self._raw_max_daily = record
            if self.sun_set_rise.sunrise() < record.timestamp < self.sun_set_rise.sunset():
                _day.append(record)
                if not self._raw_min_day or self._raw_min_day.temperature > record.temperature:
                    self._raw_min_day = record
                if not self._raw_max_day or self._raw_max_day.temperature < record.temperature:
                    self._raw_max_day = record
            else:
                _night.append(record)
                if not self._raw_min_night or self._raw_min_night.temperature > record.temperature:
                    self._raw_min_night = record
                if not self._raw_max_night or self._raw_max_night.temperature < record.temperature:
                    self._raw_max_night = record

        if len(self._raw_records) > 0:
            self._raw_avg_daily = stats.tmean([_r.temperature for _r in self._raw_records])
        if len(_day) > 0:
            self._raw_avg_day = stats.tmean([_r.temperature for _r in _day])
        if len(_night) > 0:
            self._raw_avg_night = stats.tmean([_r.temperature for _r in _night])

        if len(_day) > 0:
            if self._raw_min_night is None:
                self._raw_min_night = _day[0] if _day[0].temperature < _day[-1].temperature else _day[-1]
                self._raw_min_night.timestamp = self.sun_set_rise.sunrise() \
                    if _day[0].temperature < _day[-1].temperature else self.sun_set_rise.sunset()
                if self._is_for_today and self._raw_min_night.timestamp > self.the_date:
                    self._raw_min_night.timestamp = self.sun_set_rise.sunrise()
            if self._raw_max_night is None:
                self._raw_max_night = _day[0] if _day[0].temperature > _day[-1].temperature else _day[-1]
                self._raw_max_night.timestamp = self.sun_set_rise.sunrise() \
                    if _day[0].temperature > _day[-1].temperature else self.sun_set_rise.sunset()
                if self._is_for_today and self._raw_max_night.timestamp > self.the_date:
                    self._raw_max_night.timestamp = self.sun_set_rise.sunrise()
            if self._raw_avg_night is None:
                self._raw_avg_night = (_day[0].temperature + _day[-1].temperature)/2

        if not self._is_for_today or self.the_date > self.sun_set_rise.sunrise():
            if self._raw_min_night is not None and self._raw_min_day is None:
                self._raw_min_day = self._raw_min_night
                self._raw_min_day.timestamp = self.sun_set_rise.sunset()
            if self._raw_max_night is not None and self._raw_max_day is None:
                self._raw_max_day = self._raw_max_night
                self._raw_max_day.timestamp = self.sun_set_rise.sunset()
            if self._raw_avg_night is not None and self._raw_avg_day is None:
                self._raw_avg_day = self._raw_avg_night

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
        self._ph_min_temperatures = list()
        self._ph_max_temperatures = list()

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
            if len(self._ph_temperatures) > 0:
                _min = min([observation.temperature for observation in observations])
                _max = max([observation.temperature for observation in observations])
                self._ph_errors_lower.append(abs(self._ph_temperatures[-1] - _min))
                self._ph_errors_upper.append(abs(self._ph_temperatures[-1] - _max))
                self._ph_min_temperatures.append(_min)
                self._ph_max_temperatures.append(_max)

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

    def get_perhour_min_temperatures(self) -> list:
        if not self._ph_min_temperatures:
            self._init_ph()

        return self._ph_min_temperatures

    def get_perhour_max_temperatures(self) -> list:
        if not self._ph_max_temperatures:
            self._init_ph()

        return self._ph_max_temperatures

    def get_min_daily(self) -> tuple:
        if self._raw_min_daily is None:
            self._init_raw()
        return (self._raw_min_daily.timestamp, self._raw_min_daily.temperature) \
            if self._raw_min_daily is not None else (None, None)

    def get_max_daily(self) -> tuple:
        if self._raw_max_daily is None:
            self._init_raw()
        return (self._raw_max_daily.timestamp, self._raw_max_daily.temperature) \
            if self._raw_max_daily is not None else (None, None)

    def get_min_day(self) -> tuple:
        if self._raw_min_day is None:
            self._init_raw()
        return (self._raw_min_day.timestamp, self._raw_min_day.temperature) \
            if self._raw_min_day is not None else (None, None)

    def get_max_day(self) -> tuple:
        if self._raw_max_day is None:
            self._init_raw()
        return (self._raw_max_day.timestamp, self._raw_max_day.temperature) \
            if self._raw_max_day is not None else (None, None)

    def get_min_night(self) -> tuple:
        if self._raw_min_night is None:
            self._init_raw()
        return (self._raw_min_night.timestamp, self._raw_min_night.temperature) \
            if self._raw_min_night is not None else (None, None)

    def get_max_night(self) -> tuple:
        if self._raw_max_night is None:
            self._init_raw()
        return (self._raw_max_night.timestamp, self._raw_max_night.temperature) \
            if self._raw_max_night is not None else (None, None)

    def get_avg_daily(self) -> float:
        if self._raw_avg_daily is None:
            self._init_raw()
        return self._raw_avg_daily

    def get_avg_day(self) -> float:
        if self._raw_avg_day is None:
            self._init_raw()
        return self._raw_avg_day

    def get_avg_night(self) -> float:
        if self._raw_avg_night is None:
            self._init_raw()
        return self._raw_avg_night

    def get_lower_errors(self) -> list:
        if not self._ph_errors_lower:
            self._init_ph()
        return self._ph_errors_lower

    def get_upper_errors(self) -> list:
        if not self._ph_errors_upper:
            self._init_ph()
        return self._ph_errors_upper


class RainLastHours:

    def __init__(self, records: list, the_date: datetime, hours_in_the_past: int):
        """
        Constructs time-line, a collection of time marks, each one at round hour
        starting from housrs_count before the_date and ending at the_date (with minutes and seconds zeroed).
        For each of these "hours" a count of impulses produced by Rain Gauge is returned
        :param records list of datetime, the exact time when single impulse was recorded
        :param the_date: the latest date till the statistics are produced
        :param hours_in_the_past: number of hours "in the past" respecting the_date
        """

        self._records = records
        self.the_date = the_date
        self.hours_in_the_past = hours_in_the_past

        _start = the_date.replace(minute=0, second=0, microsecond=0)
        self._timeline = [_start + timedelta(hours=h-hours_in_the_past+1) for h in range(hours_in_the_past)]
        _marks_ids_by_key = {self._timeline[_i].strftime('%Y%m%d%H'): _i for _i in range(len(self._timeline))}
        self._counts = [0 for _m in self._timeline]
        for _d in records:
            _m_i = _marks_ids_by_key[_d.strftime('%Y%m%d%H')]
            self._counts[_m_i] = self._counts[_m_i] + 1

    def get_percipitation_per_hour_mm(self, mm_per_impulse: float) -> list:
        """
        Returns list of percipitation in mm recorded in subsequent hours - aligned to timeline.
        :param mm_per_impulse: externally provided (becasue coming from configuration) the mm/impulse value
        :return: list of floats, percipitation in mm recorded in subsequent hours
        """
        return [mm_per_impulse * _impulses for _impulses in self._counts]

    def get_percipitation_per_hour_timeline(self) -> list:
        """
        Returns the round-hours timeline for which the observations are aligned
        :return: list of datetimes
        """
        return self._timeline

    def get_percipitation_mm(self, mm_per_impulse: float) -> float:
        """
        Returns total percipitation recorded within this period.
        :param mm_per_impulse:
        :return: float, in mm, the total percipitation
        """
        return mm_per_impulse * sum(self._counts)


class TemperatureStatistics:

    def __init__(self,  _the_date: datetime):
        pass


# ----------------------------------------------------------------------------------------------------------------------


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
