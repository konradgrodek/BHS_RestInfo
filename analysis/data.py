"""
This encapsulates all classes / methods used to prepare data stored in the database for further analysis (or just show)
"""
from scipy import stats
from functools import reduce

from persistence.analysis import *
from core.util import SunsetCalculator


class AnalysisDataSource:

    def __init__(self, db: str, host: str, user: str, password: str):
        self.persistence = AnalysisPersistence(db=db, host=host, user=user, password=password)

    def temperature_daily_chronology(
            self, sensor_location: str, the_date: datetime = datetime.now()):
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

    def cesspit_increase(self, the_date: datetime, tank_full_mm: int, tank_empty_mm: int, hours_in_past: int = 0, days_in_past: int = 0, add_adherent_records: bool = False):
        """
        Returns collection of cesspit level observations within
        period given by end-date and number of preceeding days / hours
        :param the_date: the end date of the time window of interest
        :param tank_full_mm: the level, which is assumed to reported for full tank
        :param tank_empty_mm: the level, which is assumed to reported for empty tank
        :param hours_in_past: number of hours in past to be considered when calculating start date of observation window
        :param days_in_past: number of days in past to be considered when calculating start date of observation window
        :param add_adherent_records if True, the adherent records in past and future will be added to the result set
        :return: CesspitHistory object initialized with records from given observation window
        """
        if hours_in_past is None:
            hours_in_past = 0
        if days_in_past is None:
            days_in_past = 0
        if hours_in_past == 0 and days_in_past == 0:
            raise ValueError(
                f'Fetching cesspit observations require at least one: hours-in-past or days-in-past to be non-zero')
        if the_date is None:
            raise ValueError(f'The end-date is required to be provided when fetching cesspit observations')

        _hours_count = hours_in_past + 24 * days_in_past
        _records = self.persistence.cesspit_observations(
                the_date,
                hours_count=_hours_count,
                full_day_included=days_in_past > 0
            )

        adherent_rec_next = None
        adherent_rec_prev = None
        if add_adherent_records:
            _boundry_up = the_date if len(_records) == 0 else _records[-1].timestamp
            _boundry_down = (the_date - timedelta(hours=_hours_count)).replace(minute=0, second=0, microsecond=0) \
                if len(_records) == 0 else _records[0].timestamp
            adherent_rec_next = self.persistence.cesspit_adherent_observation(_boundry_up, is_next=True)
            adherent_rec_prev = self.persistence.cesspit_adherent_observation(_boundry_down, is_next=False)

        return CesspitHistory(
            records=_records,
            adherent_previous_record=adherent_rec_prev,
            adherent_next_record=adherent_rec_next,
            tank_full_mm=tank_full_mm,
            tank_empty_mm=tank_empty_mm
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


class CesspitHistory:
    """
    The class is responsible to translate the collection of cesspit levels readings into data ready to show on charts
    """

    def __init__(self, records: list, adherent_previous_record: TankLevel, adherent_next_record: TankLevel, tank_full_mm: int, tank_empty_mm: int):
        """
        Initializes the object and some of the pre-calculated data:
        - detects the tank removals and splits the timeline into different periods
        - calculates the deltas of levels.
        By providing the records that direcly adhere to range under consideration, "artificial records"
        are added with the timestamp modified, pointing to 00:00:00 of the first record and 23:59:59 of the last record.
        This is to avoid continuous charts starting in the middle of the day.
        Note that levels and timelines aggregated per hour and day are only initialized with empty lists.
        They are calculated when first used
        :param records: the records as read from the database
        :param adherent_previous_record the record that directly precedes the range under consideration
        :param adherent_next_record the record that directly follows the range under consideration
        :param tank_full_mm: the level, which is assumed to reported for full tank
        :param tank_empty_mm: the level, which is assumed to reported for empty tank
        """
        self._records = records.copy()
        self._tank_full_mm = tank_full_mm
        self._tank_empty_mm = tank_empty_mm
        _timeline = [_r.timestamp for _r in records]
        _levels = [_r.level if _r.level > self._tank_full_mm else self._tank_full_mm for _r in records]
        if adherent_previous_record is not None:
            self._records = [adherent_previous_record] + self._records
            _timeline = [adherent_previous_record.timestamp if len(records) < 1
                         else records[0].timestamp.replace(hour=0, minute=0, second=0)] + _timeline
            _levels = [adherent_previous_record.level] + _levels
        if adherent_next_record is not None:
            self._records.append(adherent_next_record)
            _timeline.append(adherent_next_record.timestamp if len(records) < 1
                             else records[-1].timestamp.replace(hour=23, minute=59, second=59))
            _levels.append(adherent_next_record.level)

        # delta is "reversed" because levels are lowering (this is the "space left", not "space occupied")
        _deltas = [prv - nxt for prv, nxt in zip(_levels[:-1], _levels[1:])]
        # to make things easier, calculate indices of negative delta both for _deltas and _timelines
        _removals_d = [_i for _i, _d in zip(range(len(_deltas)), _deltas) if _d < 0]
        _removals_tm = [_i+1 for _i in _removals_d]
        # continuous timeline and deltas
        self._continuous_timelines = \
            [_timeline[_ib:_ie] for _ib, _ie in zip([0] + _removals_tm, _removals_tm + [len(_timeline)])]
        self._continuous_levels = \
            [_levels[_ib:_ie] for _ib, _ie in zip([0] + _removals_tm, _removals_tm + [len(_timeline)])]

        # this will keep only the timestamps relevant for deltas
        # it will be at least one element shorter than continuous
        self._deltas_timeline = reduce(
            lambda x, y: x+y,
            [_timeline[_ib+1:_ie] for _ib, _ie in zip([0]+_removals_tm, _removals_tm+[len(_timeline)])])
        self._deltas = reduce(
            lambda x, y: x+y,
            [_deltas[_ib+1:_ie] for _ib, _ie in zip([-1]+_removals_d, _removals_d+[len(_deltas)])])

        self._deltas_timeline_ph = []
        self._deltas_ph = []

        self._deltas_timeline_pd = []
        self._deltas_pd = []

    def is_valid(self):
        return len(self._records) > 0 \
            and self._tank_full_mm is not None \
            and self._tank_empty_mm is not None \
            and self._tank_full_mm < self._tank_empty_mm

    @staticmethod
    def _cluster(clusters_top_values: list, objects_to_cluster: list, get_clustering_attr, get_value_attr):
        """
        Divides the input list of objects into clusters accordingly to defined boundaries.
        Returns list of lists, where top list is of size top-values + 1;
        each element of the list is also list (a cluster), composed of elements assigned to the cluster.
        For example, if the top-values = [2022-09-17, 2022-09-18], the result will be in following format:
        [
            [all-objects with get_clustering_attr < 2022-09-17],
            [2022-09-17 < get_clustering_attr <= 2022-09-18]
            [get_clustering_attr > 2022-09-18]
        ]
        Again, note that result set is longer than provided top-boundaries!
        :param clusters_top_values: defines the top boundaries of the clusters
        :param objects_to_cluster: the objects to be clustered
        :param get_clustering_attr: the method which will be applied on the object to get value,
        which can be compared with the boundaries
        :param get_value_attr: the method, which will be applied to the object to get value,
        which will be appended to the cluster
        :return: list of lists of objects returned by get_value_attr
        """
        _clusters_def = list(zip([None] + clusters_top_values, clusters_top_values + [None],
                            range(len(clusters_top_values)+1)))
        _clustered = [list() for _i in range(len(clusters_top_values)+1)]
        for _vattr, _val in [(get_clustering_attr(_v), get_value_attr(_v)) for _v in objects_to_cluster]:
            for _cluster_def in _clusters_def:
                if (_cluster_def[0] is None or _vattr > _cluster_def[0]) \
                        and (_cluster_def[1] is None or _vattr <= _cluster_def[1]):
                    _clustered[_cluster_def[2]].append(_val)
                    break
        return _clustered

    def _fill_perc(self, level_mm: int) -> float:
        """
        For given measured level calculates what is actual fill percentage of the tank
        :param level_mm: the measured level (measurement is from the top!)
        :param tank_full_mm: the level when the tank is full
        :param tank_empty_mm: the level, in millimeters, when the tank is assumed to be empty
        :return: the fill percenage of the cesspit
        """
        return 100 * (self._tank_empty_mm - level_mm) / (self._tank_empty_mm - self._tank_full_mm)

    def _init_ph(self):
        """
        Lazy-initialization of the aggregates per-hour
        :return: nothing
        """
        # per-hour timeline
        _start = self._records[0].timestamp.replace(minute=0, second=0, microsecond=0)
        _stop = self._records[-1].timestamp
        _hours_count = int((_stop - _start).total_seconds() / (60 * 60)) + 1
        self._deltas_timeline_ph = [_start + timedelta(hours=h) for h in range(_hours_count)]
        _deltas_clusterd_ph = CesspitHistory._cluster(
            self._deltas_timeline_ph[1:], list(zip(self._deltas_timeline, self._deltas)), lambda x: x[0], lambda x: x[1])
        self._deltas_ph = [0 if len(_cl) == 0 else sum(_cl) for _cl in _deltas_clusterd_ph]

    def _init_pd(self):
        """
        Lazy-initializtion of the per-day aggregates
        :return: nothing
        """
        # per-day timeline
        _start = self._records[0].timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        _stop = self._records[-1].timestamp
        _days_count = int((_stop - _start).total_seconds() / (24 * 60 * 60)) + 1
        self._deltas_timeline_pd = [_start + timedelta(days=d) for d in range(_days_count)]
        _deltas_clustered_pd = CesspitHistory._cluster(
            self._deltas_timeline_pd[1:], list(zip(self._deltas_timeline, self._deltas)), lambda x: x[0], lambda x: x[1])
        self._deltas_pd = [0 if len(_cl) == 0 else sum(_cl) for _cl in _deltas_clustered_pd]

    def get_continuous_timeline(self) -> list:
        """
        Returns all timelines, in separated lists, for each cycles detected within period
        (cycles are separated by cesspit removals)
        :return: list of lists of datetime (time marks of readings)
        """
        return self._continuous_timelines

    def get_continuous_levels(self) -> list:
        """
        Returns all levels within given period, divided into cycles - therefore there could be more than one
        :return: list of list with readings - note: these are original levels, i.e. the measured distances
        from sensor to the sevage level
        """
        return self._continuous_levels

    def get_continuous_fill_perc(self) -> list:
        """
        Reuturns all the fill levels (in percentages) of the cesspit within given period,
        split by removals (if any detected)
        :return: list of lists of floats (percentages)
        """
        return [
            [self._fill_perc(_lvl) for _lvl in _sub_line]
            for _sub_line in self._continuous_levels]

    def get_per_hour_timeline(self) -> list:
        """
        Returns "full hour only" time-marks within given period. Note: these are not read readings,
        these are generated artificially full-hours within given period.
        :return: list of datetime, all full-hours within given period, without gaps
        """
        if len(self._deltas_timeline_ph) == 0:
            self._init_ph()
        return self._deltas_timeline_ph

    def get_per_hour_deltas(self) -> list:
        """
        Calculates (if not done yet) and returns aggregations per each full hour how much available space
        in the cesspit decreased
        :return: list of ints of the same size as get_per_hour_timeline returns
        """
        if len(self._deltas_ph) == 0:
            self._init_ph()
        return self._deltas_ph

    def get_per_hour_deltas_perc(self) -> list:
        """
        Similar to get_per_hour_deltas, but the decrease levels are translated into percentages of the whole tank
        :return: list of floats (percentages) of the same size as get_per_hour_timeline returns
        """
        if len(self._deltas_ph) == 0:
            self._init_ph()
        return [100 * _d / (self._tank_empty_mm - self._tank_full_mm) for _d in self._deltas_ph]

    def get_per_day_timeline(self) -> list:
        """
        Returns time marks, one per each day within given period in form YYYY-MM-DD 00:00:00
        :return: the list of "full day" time marks
        """
        if len(self._deltas_timeline_pd) == 0:
            self._init_pd()
        return self._deltas_timeline_pd

    def get_per_day_deltas(self) -> list:
        """
        Returns aggregated (summed up) decreases of cesspit levels per each day within given period
        :return: list of int (deltas, in mm, how much the available space decreased daily)
        """
        if len(self._deltas_pd) == 0:
            self._init_pd()
        return self._deltas_pd

    def get_per_day_deltas_perc(self) -> list:
        """
        Prepares (if needed) and returns the daily decrease in percentages of full tank of the available space left
        :return: list[float]
        """
        if len(self._deltas_pd) == 0:
            self._init_pd()
        return [100*_d/(self._tank_empty_mm - self._tank_full_mm) for _d in self._deltas_pd]


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

# ----------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    db = analysis_data_source()
    t = db.cesspit_increase(the_date=datetime.now(), hours_in_past=7*24+15)
    t = db.cesspit_increase(the_date=datetime.now(), days_in_past=28)
    print(t.get_per_day_timeline())
    print(t.get_per_day_deltas())
    print(t.get_per_hour_timeline())
    print(t.get_per_hour_deltas())

