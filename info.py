from flask import Flask
from configparser import ConfigParser, ExtendedInterpolation
import sys
import requests

from core.sbean import *
from core.util import SunsetCalculator
from persistence.schema import *


class Configuration(ConfigParser):
    PATH_INI = '/etc/bhs/rest-info/rest-info.ini'
    PATH_ENV = '/etc/bhs/rest-info/env.ini'

    PATH_INI_DEBUG = './test/test.config.ini'
    PATH_ENV_DEBUG = '../BHS_Deployment/install/.credentials'

    SECTION_DB = 'DATABASE'
    SECTION_TEMPERATURE = 'TEMPERATURE'
    SECTION_PRESSURE = 'PRESSURE'
    SECTION_AIRQUALITY = 'AIR-QUALITY'
    SECTION_HUMIDITY = 'HUMIDITY'
    SECTION_CESSPIT = 'CESSPIT'
    SECTION_DAYLIGHT = 'DAYLIGHT'
    SECTION_RAIN = 'RAIN'

    PARAM_DB = 'db'
    PARAM_USER = 'user'
    PARAM_PASSWORD = 'password'
    PARAM_HOST = 'host'
    PARAM_AQ_NORM_2_5 = 'norm.pm_2_5'
    PARAM_AQ_NORM_10 = 'norm.pm_10'
    PARAM_WARNING_LEVEL = 'warning-level'
    PARAM_CRITICAL_LEVEL = 'critical-level'
    PARAM_DELAY_DENOTING_FAILURE = 'delay-denoting-failure-min'

    def __init__(self):
        ConfigParser.__init__(self, interpolation=ExtendedInterpolation())
        if not sys.gettrace():
            self.read([self.PATH_ENV, self.PATH_INI])
        else:
            self.read([self.PATH_ENV_DEBUG, self.PATH_INI_DEBUG])

    def get_db(self) -> str:
        return self.get(section=self.SECTION_DB, option=self.PARAM_DB)

    def get_db_user(self) -> str:
        return self.get(section=self.SECTION_DB, option=self.PARAM_USER)

    def get_db_password(self) -> str:
        return self.get(section=self.SECTION_DB, option=self.PARAM_PASSWORD)

    def get_db_host(self) -> str:
        return self.get(section=self.SECTION_DB, option=self.PARAM_HOST)

    def get_temperature_hosts(self) -> list:
        return [self.get(section=self.SECTION_TEMPERATURE, option=option)
                for option in self.options(self.SECTION_TEMPERATURE)
                if option.startswith(self.PARAM_HOST)]

    def get_pressure_host(self) -> str:
        return self.get(section=self.SECTION_PRESSURE, option=self.PARAM_HOST)

    def get_humidity_host(self) -> str:
        return self.get(section=self.SECTION_HUMIDITY, option=self.PARAM_HOST)

    def get_air_quality_host(self) -> str:
        return self.get(section=self.SECTION_AIRQUALITY, option=self.PARAM_HOST)

    def get_air_quality_norm_pm_2_5(self) -> int:
        return self.getint(section=self.SECTION_AIRQUALITY, option=self.PARAM_AQ_NORM_2_5)

    def get_air_quality_norm_pm_10(self) -> int:
        return self.getint(section=self.SECTION_AIRQUALITY, option=self.PARAM_AQ_NORM_10)

    def get_cesspit_host(self) -> str:
        return self.get(section=self.SECTION_CESSPIT, option=self.PARAM_HOST)

    def get_cesspit_warning_level(self) -> float:
        return self.getfloat(section=self.SECTION_CESSPIT, option=self.PARAM_WARNING_LEVEL)

    def get_cesspit_critical_level(self) -> float:
        return self.getfloat(section=self.SECTION_CESSPIT, option=self.PARAM_CRITICAL_LEVEL)

    def get_cesspit_delay_denoting_failure_min(self) -> int:
        return self.getint(section=self.SECTION_CESSPIT, option=self.PARAM_DELAY_DENOTING_FAILURE)

    def get_daylight_host(self) -> str:
        return self.get(section=self.SECTION_DAYLIGHT, option=self.PARAM_HOST)

    def get_rain_host(self) -> str:
        return self.get(section=self.SECTION_RAIN, option=self.PARAM_HOST)


class InfoApp(Flask):

    def __init__(self, name):
        Flask.__init__(self, import_name=name)
        self.info_config = Configuration()
        self.persistence = Persistence(
            db=self.info_config.get_db(),
            user=self.info_config.get_db_user(),
            password=self.info_config.get_db_password(),
            host=self.info_config.get_db_host())

    def _make_request(self, endpoint: str) -> tuple:
        if not endpoint.startswith('http://'):
             endpoint = 'http://' + endpoint

        response = None
        error = None

        try:
            response = requests.get(endpoint)
            # this will raise HttpError on erroneous responses:
            response.raise_for_status()
            # ...and this will also trigger error for unexpected (although non-erroneous) status codes
            if response.status_code != 200:
                error = ErrorJsonBean(f'Non-typical HTTP response code detected: {response.status_code}')

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            error = NotAvailableJsonBean()

        except requests.exceptions.TooManyRedirects:
            error = ErrorJsonBean(f'Host {endpoint} is not responding correctly. '
                                  f'Too many redirects (?)')

        except requests.exceptions.HTTPError as e:
            error = ErrorJsonBean(f'Host {endpoint} is responding, but with erroneous code. '
                                  f'Details: {str(e)}')

        except requests.exceptions.RequestException as e:
            error = ErrorJsonBean(f'Host {endpoint} is not responding correctly. '
                                  f'Details: {str(type(e))}: {str(e)}')

        return error, response

    def current_temperature(self):
        hosts = self.info_config.get_temperature_hosts()

        consolidated_response = list()
        # make requests to all hosts
        for host in hosts:
            error, response = self._make_request(host)
            if not error and response:
                consolidated_response.extend(json_to_bean(response.json()))

        if len(consolidated_response) < 1:
            return bean_jsonified(NotAvailableJsonBean())

        return bean_jsonified([self.convert_temperature(t) for t in consolidated_response])

    def convert_temperature(self, temp: TemperatureReadingJson) -> TemperatureReadingJson:
        """
        This method is intended to adapt temperature from the raw reading to human-readable format

        :param temp: the original temperature reading
        :return: the same bean, but with modified content
        """
        temp.temperature = int(temp.temperature*10.0)/10.0
        return temp

    def current_pressure(self):
        error, response = self._make_request(self.info_config.get_pressure_host())

        if error:
            return bean_jsonified(error)

        pressure = json_to_bean(response.json())
        pressure.current_value = int(pressure.current_value)
        pressure.current_mean = int(pressure.current_mean)
        pressure.previous_mean = int(pressure.previous_mean)

        return bean_jsonified(pressure)

    def current_humidity(self):
        error, response = self._make_request(self.info_config.get_humidity_host())

        if error:
            return bean_jsonified(error)

        humidity = json_to_bean(response.json())
        humidity.current_value = int(humidity.current_value)
        humidity.current_mean = int(humidity.current_mean)
        humidity.previous_mean = int(humidity.previous_mean)

        return bean_jsonified(humidity)

    def current_air_quality(self):
        error, response = self._make_request(self.info_config.get_air_quality_host())

        if error:
            return bean_jsonified(error)

        return bean_jsonified(AirQualityInterpretedReadingJson(
            reading=json_to_bean(response.json()),
            norm={AirQualityInterpretedReadingJson.PM_10: self.info_config.get_air_quality_norm_pm_10(),
                  AirQualityInterpretedReadingJson.PM_2_5: self.info_config.get_air_quality_norm_pm_2_5()}))

    def current_cesspit_level(self):
        error, response = self._make_request(self.info_config.get_cesspit_host())

        if error:
            return bean_jsonified(error)

        reading = json_to_bean(response.json())
        failure = (datetime.now() - reading.timestamp).total_seconds()/60 > \
            self.info_config.get_cesspit_delay_denoting_failure_min()

        return bean_jsonified(CesspitInterpretedReadingJson(
            reading=reading,
            failure_detected=failure,
            warning_level_perc=self.info_config.get_cesspit_warning_level(),
            critical_level_perc=self.info_config.get_cesspit_critical_level()))

    def current_daylight_status(self):
        error, response = self._make_request(self.info_config.get_daylight_host())

        if error:
            return bean_jsonified(error)

        _reading = json_to_bean(response.json())

        # calculate time-of-day, set sunset\sunshine
        _tod = TimeOfDay.NIGHT
        _calc = SunsetCalculator()

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

    def current_rain_status(self):
        error, response = self._make_request(self.info_config.get_rain_host())

        if error:
            return bean_jsonified(error)

        rain_intensity = json_to_bean(response.json())

        return bean_jsonified(rain_intensity)

    def pass_by(self, host):
        error, response = self._make_request(host)

        if error:
            return bean_jsonified(error)

        return jsonify(response.json())


# flask config
app = InfoApp(__name__)


@app.route('/current/temperature')
def current_temperature():
    return app.current_temperature()


@app.route('/current/pressure')
def current_pressure():
    return app.current_pressure()


@app.route('/current/humidity_in')
def current_humidity_in():
    return app.current_humidity()


@app.route('/current/air_quality')
def current_air_quality():
    return app.current_air_quality()


@app.route('/current/cesspit')
def current_cesspit_level():
    return app.current_cesspit_level()


@app.route('/current/daylight')
def current_daylight_status():
    return app.current_daylight_status()


@app.route('/current/rain')
def current_rain_status():
    return app.current_rain_status()


if __name__ == '__main__':
    # in debug mode start the server using flask built-in server
    # of course for production use mod-wsgi_express on Apache2
    from werkzeug.serving import make_server

    # start on different port (production is 12000 to avoid conflicts)
    server = make_server('0.0.0.0', 12999, app)
    context = app.app_context()
    context.push()
    server.serve_forever()
