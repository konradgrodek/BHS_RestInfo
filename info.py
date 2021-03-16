from flask import Flask
from configparser import ConfigParser, ExtendedInterpolation
import sys
import requests

from core.bean import *
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

    PARAM_DB = 'db'
    PARAM_USER = 'user'
    PARAM_PASSWORD = 'password'
    PARAM_HOST = 'host'
    PARAM_AQ_NORM_2_5 = 'norm.pm_2_5'
    PARAM_AQ_NORM_10 = 'norm.pm_10'

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


class InfoApp(Flask):

    def __init__(self, name):
        Flask.__init__(self, import_name=name)
        self.info_config = Configuration()
        self.persistence = Persistence(
            db=self.info_config.get_db(),
            user=self.info_config.get_db_user(),
            password=self.info_config.get_db_password(),
            host=self.info_config.get_db_host())

    def current_temperature(self):
        hosts = self.info_config.get_temperature_hosts()

        consolidated_response = list()
        # make requests to all hosts
        for host in hosts:
            r = requests.get(f'http://{host}')
            if r.status_code == 200:
                consolidated_response.extend(json_to_bean(r.json()))

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
        ori_r = requests.get(f'http://{self.info_config.get_pressure_host()}')
        if ori_r.status_code > 500:
            return bean_jsonified(ErrorJsonBean(_error=f'Host {self.info_config.get_pressure_host()} '
                                                       f'returned HTTP status {ori_r.status_code}'))
        elif ori_r.status_code != 200:
            return bean_jsonified(NotAvailableJsonBean())

        pressure = json_to_bean(ori_r.json())
        pressure.current_value = int(pressure.current_value)
        pressure.current_mean = int(pressure.current_mean)
        pressure.previous_mean = int(pressure.previous_mean)

        return bean_jsonified(pressure)

    def current_humidity(self):
        ori_r = requests.get(f'http://{self.info_config.get_humidity_host()}')
        if ori_r.status_code > 500:
            return bean_jsonified(ErrorJsonBean(_error=f'Host {self.info_config.get_humidity_host()} '
                                                       f'returned HTTP status {ori_r.status_code}'))
        elif ori_r.status_code != 200:
            return bean_jsonified(NotAvailableJsonBean())

        humidity = json_to_bean(ori_r.json())
        humidity.current_value = int(humidity.current_value)
        humidity.current_mean = int(humidity.current_mean)
        humidity.previous_mean = int(humidity.previous_mean)

        return bean_jsonified(humidity)

    def current_air_quality(self):
        ori_r = requests.get(f'http://{self.info_config.get_air_quality_host()}')
        if ori_r.status_code > 500:
            return bean_jsonified(ErrorJsonBean(_error=f'Host {self.info_config.get_air_quality_host()} '
                                                       f'returned HTTP status {ori_r.status_code}'))
        elif ori_r.status_code != 200:
            return bean_jsonified(NotAvailableJsonBean())

        return bean_jsonified(AirQualityInterpretedReadingJson(
            reading=json_to_bean(ori_r.json()),
            norm={AirQualityInterpretedReadingJson.PM_10: self.info_config.get_air_quality_norm_pm_10(),
                  AirQualityInterpretedReadingJson.PM_2_5: self.info_config.get_air_quality_norm_pm_2_5()}))

    def pass_by(self, host):
        ori_r = requests.get(f'http://{host}')
        if ori_r.status_code > 500:
            return bean_jsonified(ErrorJsonBean(_error=f'Host {host} returned HTTP status {ori_r.status_code}'))
        elif ori_r.status_code != 200:
            return bean_jsonified(NotAvailableJsonBean())

        return jsonify(ori_r.json())


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


if __name__ == '__main__':
    # in debug mode start the server using flask built-in server
    # of course for production use mod-wsgi_express on Apache2
    from werkzeug.serving import make_server

    # start on different port (production is 12000 to avoid conflicts)
    server = make_server('0.0.0.0', 12999, app)
    context = app.app_context()
    context.push()
    server.serve_forever()
