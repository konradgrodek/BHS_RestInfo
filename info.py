from flask import Flask, Response
from flask import request as frequest
from datetime import datetime

from localconfig import Configuration
import remote
import analysis.data as a_data
import analysis.graph as a_graph


class InfoApp(Flask):

    def __init__(self, name):
        Flask.__init__(self, import_name=name)
        self.info_config = Configuration()
        self.data_source = a_data.AnalysisDataSource(
            db=self.info_config.get_db(),
            user=self.info_config.get_db_user(),
            password=self.info_config.get_db_password(),
            host=self.info_config.get_db_host())
        self.remote_temperature = remote.Temperature(self.info_config.get_temperature_hosts())
        self.remote_pressure = remote.TendencyValue(self.info_config.get_pressure_host())
        self.remote_humidity_in = remote.TendencyValue(self.info_config.get_humidity_host())
        self.remote_air_quality = remote.AirQuality(self.info_config.get_air_quality_host(),
                                                    norm_pm_10=self.info_config.get_air_quality_norm_pm_10(),
                                                    norm_pm_2_5=self.info_config.get_air_quality_norm_pm_2_5())
        self.remote_cesspit = remote.Cesspit(self.info_config.get_cesspit_host(),
                                             delay_denoting_failure_min=self.info_config.get_cesspit_delay_denoting_failure_min(),
                                             warning_level=self.info_config.get_cesspit_warning_level(),
                                             critical_level=self.info_config.get_cesspit_critical_level())
        self.remote_daylight = remote.Daylight(self.info_config.get_daylight_host())
        self.remote_rain = remote.Rain(self.info_config.get_rain_host())
        self.remote_soil_moisture = remote.SoilMoisture(self.info_config.get_soil_moisture_host())

    def current_temperature(self):
        return self.remote_temperature.current_temperature()

    def current_pressure(self):
        return self.remote_pressure.current_value()

    def current_humidity(self):
        return self.remote_humidity_in.current_value()

    def current_air_quality(self):
        return self.remote_air_quality.current_value()

    def current_cesspit_level(self):
        return self.remote_cesspit.current_value()

    def current_daylight_status(self):
        return self.remote_daylight.current_status()

    def current_rain_status(self):
        return self.remote_rain.current_status()

    def current_soil_moisture(self):
        return self.remote_soil_moisture.current_humidity()

    @staticmethod
    def _datetime_fmt(to_convert: str):
        return datetime.strptime(to_convert, '%Y-%m-%d')

    def graph_temperature(self):
        _sensor_location = frequest.args.get('location')
        _the_date = frequest.args.get('date', type=self._datetime_fmt)
        _title = frequest.args.get('title')

        _now = datetime.now()
        _is_for_today = not _the_date or (_the_date.year == _now.year and
                                          _the_date.month == _now.month and
                                          _the_date.day == _now.day)

        if not _the_date or _is_for_today:
            _the_date = _now

        # current temperature will be displayed only when displaying results for today
        cur_temp = None
        if _is_for_today:
            cur_temp_reading = self.remote_temperature.current_temperature_for_sensor(_sensor_location)
            if cur_temp_reading:
                cur_temp = cur_temp_reading.timestamp, cur_temp_reading.temperature
        else:
            _the_date = _the_date.replace(hour=23, minute=59, second=59)

        _graph = a_graph.DailyTemperatureGraph(
            data_source=self.data_source,
            sensor_loc=_sensor_location,
            title=_title,
            last_temp=cur_temp,
            the_date=_the_date)

        return Response(_graph.plot_to_svg(), mimetype='image/svg+xml')


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


@app.route('/current/soil_moisture')
def current_soil_moisture():
    return app.current_soil_moisture()


@app.route('/graph/temperature')
def graph_temperature():
    return app.graph_temperature()


if __name__ == '__main__':
    # in debug mode start the server using flask built-in server
    # of course for production use mod-wsgi_express on Apache2
    from werkzeug.serving import make_server

    # start on different port (production is 12000 to avoid conflicts)
    server = make_server('0.0.0.0', 12999, app)
    context = app.app_context()
    context.push()
    server.serve_forever()
