"""
Collects all routines related to communication with remote services, mainly to obtain current, close to runtime values
"""
import requests

from core.sbean import *
from core.util import SunsetCalculator

from scipy import stats


class RemoteConnection:

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        if not self.endpoint.startswith('http://'):
            self.endpoint = 'http://' + endpoint
        self.host = endpoint
        if ':' in self.host:
            self.host = self.host[:self.host.index(':')]
        elif '/' in self.host:
            self.host = self.host[:self.host.index('/')]

    def make_request(self) -> tuple:
        response = None
        error = None

        try:
            # there's direct Gigabit interface with the service; if does not respond within 1 sec, it is surely down
            response = requests.get(self.endpoint, timeout=1)
            # this will raise HttpError on erroneous responses:
            response.raise_for_status()
            # ...and this will also trigger error for unexpected (although non-erroneous) status codes
            if response.status_code != 200:
                error = ErrorJsonBean(f'Non-typical HTTP response code detected: {response.status_code}')

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            error = NotAvailableJsonBean()

        except requests.exceptions.TooManyRedirects:
            error = ErrorJsonBean(f'Host {self.endpoint} is not responding correctly. '
                                  f'Too many redirects (?)')

        except requests.exceptions.HTTPError as e:
            error = ErrorJsonBean(f'Host {self.endpoint} is responding, but with erroneous code. '
                                  f'Details: {str(e)}')

        except requests.exceptions.RequestException as e:
            error = ErrorJsonBean(f'Host @ {self.endpoint} is not responding correctly. '
                                  f'Details: {str(type(e))}: {str(e)}')

        return error, response


class SimplyReroute(RemoteConnection):
    """
    Use this class for situations when the response from Service is completely not interpreted
    """

    def __init__(self, endpoint: str):
        RemoteConnection.__init__(self, endpoint)

    def reroute(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        if response is None:
            return bean_jsonified(NotAvailableJsonBean())

        return bean_jsonified(json_to_bean(response.json()))


class Temperature:

    def __init__(self, endpoints: list):
        self.remotes = [RemoteConnection(ep) for ep in endpoints]

    @staticmethod
    def _convert_temperature(temp: TemperatureReadingJson) -> TemperatureReadingJson:
        """
        This method is intended to adapt temperature from the raw reading to human-readable format

        :param temp: the original temperature reading
        :return: the same bean, but with modified content
        """
        temp.temperature = int(temp.temperature*10.0)/10.0
        return temp

    def _consolidated_response(self) -> list:
        consolidated_response = list()
        # make requests to all hosts
        for host in self.remotes:
            error, response = host.make_request()
            if not error and response:
                consolidated_response.extend(json_to_bean(response.json()))

        return [Temperature._convert_temperature(t) for t in consolidated_response]

    def current_temperature(self):
        """
        Returns jsonified consolidated responses from all sources of temperature readings
        :return: Flask-JSON response containing the list of current readings
        """
        consolidated_response = self._consolidated_response()

        if len(consolidated_response) < 1:
            return bean_jsonified(NotAvailableJsonBean())

        return bean_jsonified(consolidated_response)

    def current_temperature_for_sensor(self, sensor_loc: str) -> TemperatureReadingJson:
        """
        Returns the current reading (json-ready-bean, not Flask response!) for sensor of given location.
        If no such sensor reading is found, None is returned. If more then one, the first one
        :param sensor_loc: the location of the sensor
        :return: the TemperatureReadingJson if the sensor is found by location
        """
        consolidated_response = self._consolidated_response()
        reading = None
        for t in consolidated_response:
            if t.sensor_location == sensor_loc:
                reading = t
                break

        return reading


class TendencyValue(RemoteConnection):

    def __init__(self, endpoint: str):
        RemoteConnection.__init__(self, endpoint)

    def current_value(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        val = json_to_bean(response.json())
        val.current_value = int(val.current_value)
        val.current_mean = int(val.current_mean)
        val.previous_mean = int(val.previous_mean)

        return bean_jsonified(val)


class AirQuality(RemoteConnection):

    def __init__(self, endpoint: str, norm_pm_10: int, norm_pm_2_5: int):
        RemoteConnection.__init__(self, endpoint)
        self.norm_pm_10 = norm_pm_10
        self.norm_pm_2_5 = norm_pm_2_5

    def current_value(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        return bean_jsonified(AirQualityInterpretedReadingJson(
            reading=json_to_bean(response.json()),
            norm={AirQualityInterpretedReadingJson.PM_10: self.norm_pm_10,
                  AirQualityInterpretedReadingJson.PM_2_5: self.norm_pm_2_5}))


class Cesspit(RemoteConnection):

    def __init__(self, endpoint: str, delay_denoting_failure_min, warning_level, critical_level):
        RemoteConnection.__init__(self, endpoint)
        self.delay_denoting_failure_min = delay_denoting_failure_min
        self.warning_level = warning_level
        self.critical_level = critical_level

    def current_value(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        reading = json_to_bean(response.json())
        failure = (datetime.now() - reading.timestamp).total_seconds()/60 > self.delay_denoting_failure_min

        return bean_jsonified(CesspitInterpretedReadingJson(
            reading=reading,
            failure_detected=failure,
            warning_level_perc=self.warning_level,
            critical_level_perc=self.critical_level))


class CesspitConfig(RemoteConnection):

    def __init__(self, endpoint: str):
        RemoteConnection.__init__(self, endpoint)
        self.config = None

    def current_value(self):
        if self.config is not None:
            return self.config

        error, response = self.make_request()

        if error:
            return ErrorJsonBean(_error=f'The cesspit min-max levels were unable to fetch. Error code: {error}')

        self.config = json_to_bean(response.json())

        return self.config


class Daylight(RemoteConnection):

    def __init__(self, endpoint: str):
        RemoteConnection.__init__(self, endpoint)

    def current_status(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        _reading = json_to_bean(response.json())

        # calculate time-of-day, set sunset\sunshine
        _calc = SunsetCalculator()
        _tod = TimeOfDay.NIGHT

        if _calc.sunrise() < _reading.timestamp < _calc.sunset():
            _day_duration_sec = (_calc.sunset() - _calc.sunrise()).total_seconds()
            # morning is at most 20% of day duration away from sunset
            if (_reading.timestamp - _calc.sunrise()).total_seconds() < 0.2*_day_duration_sec:
                _tod = TimeOfDay.MORNING
            elif (_calc.sunset() - _reading.timestamp).total_seconds() < 0.2*_day_duration_sec:
                _tod = TimeOfDay.EVENING
            else:
                _tod = TimeOfDay.MIDDAY

        return bean_jsonified(DaylightInterpretedReadingJson(
            reading=_reading, time_of_day=_tod,
            sunrise=_calc.sunrise().strftime('%H:%M'),
            sunset=_calc.sunset().strftime('%H:%M')))


class Rain:
    """
    This interface is not valid anymore, is replaced by Precipitation
    """

    def __init__(self):
        pass

    def current_status(self):
        return bean_jsonified(ErrorJsonBean('This interface is marked as unspported. Use precipitation instead'))


class SoilMoisture(RemoteConnection):

    def __init__(self, endpoint: str):
        RemoteConnection.__init__(self, endpoint)

    def current_humidity(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        results = json_to_bean(response.json())

        for humidity in results:
            humidity.current_value = int(humidity.current_value*10.0)/10.0

        return bean_jsonified(results)


class SolarPlant(RemoteConnection):

    def __init__(self, endpoint: str, max_nominal_power: int):
        RemoteConnection.__init__(self, endpoint)
        self.maximum_nominal_power = max_nominal_power

    def current_production(self):
        """
        Calculates percentages of maximum nominal production (may return more than 100%)
        :return: the json response
        """
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        if response is None:
            return bean_jsonified(NotAvailableJsonBean())

        return bean_jsonified(SolarPlantInterpretedReadingJson(
            reading=json_to_bean(response.json()),
            maximum_nominal_power=self.maximum_nominal_power))


class Precipitation(RemoteConnection):

    def __init__(self, endpoint: str, mm_per_impulse: float):
        RemoteConnection.__init__(self, endpoint)
        self.mm_per_impulse = mm_per_impulse

    def current_precipitation(self):
        error, response = self.make_request()

        if error:
            return bean_jsonified(error)

        if response is None:
            return bean_jsonified(NotAvailableJsonBean())

        results = json_to_bean(response.json())

        _impulses = sorted(results.observations)
        _total_precipitation = self.mm_per_impulse * len(_impulses)
        _is_raining = False
        if len(_impulses) > 0:
            _time_since_last_impulse = (datetime.now() - _impulses[-1]).total_seconds()
            if len(_impulses) == 1:
                # very simple: it is raining if time from last observation is less than observation
                # duration interpreted as minutes, for example: duration is 12h, time since last should be lt 12min
                _is_raining = _time_since_last_impulse < results.observation_duration_h * 60
            else:
                # time since last observation is less than average time between impulses,
                # eliminate outliers (> 1/10 of total duration)
                try:
                    _is_raining = bool(_time_since_last_impulse < stats.tmean(
                        [(_end - _start).total_seconds() for _end, _start in zip(_impulses[1:], _impulses[:-1])],
                        limits=(0, 60*60*results.observation_duration_h/10),
                        inclusive=(False, False)))
                except ValueError:  # "No array values within given limits"
                    _is_raining = False

        return bean_jsonified(PrecipitationObservationsReadingJson(
            observation_duration_h=results.observation_duration_h,
            last_observation_at=None if len(_impulses) == 0 else _impulses[-1],
            precipitation_mm=_total_precipitation,
            is_raining=_is_raining))
