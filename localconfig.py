from configparser import ConfigParser, ExtendedInterpolation
import sys


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
    SECTION_SOIL_MOISTURE = 'SOIL-MOISTURE'
    SECTION_SOLAR_PLANT = 'SOLAR-PLANT'
    SECTION_WIND = 'WIND'
    SECTION_RAIN_GAUGE = 'RAIN-GAUGE'
    SECTION_WATER_TANK = 'WATER-TANK'

    PARAM_DB = 'db'
    PARAM_USER = 'user'
    PARAM_PASSWORD = 'password'
    PARAM_HOST = 'host'
    PARAM_AQ_NORM_2_5 = 'norm.pm_2_5'
    PARAM_AQ_NORM_10 = 'norm.pm_10'
    PARAM_WARNING_LEVEL = 'warning-level'
    PARAM_CRITICAL_LEVEL = 'critical-level'
    PARAM_DELAY_DENOTING_FAILURE = 'delay-denoting-failure-min'
    PARAM_SOLAR_PLANT_NOMINAL_POWER = 'max-nominal-power'
    PARAM_RAINGAUGE_MMPERHOUR = 'mm-per-hour'

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

    def get_soil_moisture_host(self) -> str:
        return self.get(section=self.SECTION_SOIL_MOISTURE, option=self.PARAM_HOST)

    def get_solar_plant_host(self) -> str:
        return self.get(section=self.SECTION_SOLAR_PLANT, option=self.PARAM_HOST)

    def get_solar_plant_max_nominal_power(self) -> int:
        return self.getint(section=self.SECTION_SOLAR_PLANT, option=self.PARAM_SOLAR_PLANT_NOMINAL_POWER)

    def get_wind_host(self) -> str:
        return self.get(section=self.SECTION_WIND, option=self.PARAM_HOST)

    def get_rain_gauge_host(self) -> str:
        return self.get(section=self.SECTION_RAIN_GAUGE, option=self.PARAM_HOST)

    def get_rain_gauge_mm_per_impulse(self) -> float:
        return self.getfloat(section=self.SECTION_RAIN_GAUGE, option=self.PARAM_RAINGAUGE_MMPERHOUR)

    def get_water_tank_host(self) -> str:
        return self.get(section=self.SECTION_WATER_TANK, option=self.PARAM_HOST)
