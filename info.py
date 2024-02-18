from flask import Flask, Response
from flask import request as frequest
from logging.config import dictConfig
import sys
import signal

from localconfig import Configuration
import remote
import sys_status
import analysis.data as a_data
import analysis.graph as a_graph
from core.sbean import *
from core.util import ExitEvent


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
                                             delay_denoting_failure_min=self.info_config.
                                             get_cesspit_delay_denoting_failure_min(),
                                             warning_level=self.info_config.get_cesspit_warning_level(),
                                             critical_level=self.info_config.get_cesspit_critical_level())
        self.remote_cesspit_config = remote.CesspitConfig(self.info_config.get_cesspit_config_host())
        self.remote_cesspit_log = remote.SimplyReroute(self.info_config.get_cesspit_log_host())
        self.remote_daylight = remote.Daylight(self.info_config.get_daylight_host())
        self.remote_rain = remote.Rain()
        self.remote_soil_moisture = remote.SoilMoisture(self.info_config.get_soil_moisture_host())
        self.remote_solar_plant = remote.SolarPlant(self.info_config.get_solar_plant_host(),
                                                    max_nominal_power=self.info_config.
                                                    get_solar_plant_max_nominal_power())
        self.remote_wind = remote.SimplyReroute(self.info_config.get_wind_host())
        self.remote_precipitation = remote.Precipitation(
            endpoint=self.info_config.get_rain_gauge_host(),
            mm_per_impulse=self.info_config.get_rain_gauge_mm_per_impulse())
        self.remote_water_tank = remote.SimplyReroute(self.info_config.get_water_tank_host())
        self.system_status_thread = sys_status.SystemStatusThread(self.info_config)
        self.system_status_thread.start()
        signal.signal(getattr(signal, 'SIGTERM'), self._stop_system_status_thread)

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

    def current_precipitation(self):
        return self.remote_precipitation.current_precipitation()

    def current_soil_moisture(self):
        return self.remote_soil_moisture.current_humidity()

    def current_solar_power_production(self):
        return self.remote_solar_plant.current_production()

    def current_wind(self):
        return self.remote_wind.reroute()

    def current_water_tank(self):
        return self.remote_water_tank.reroute()

    def statistics_temperature(self):
        _temperature_stats_input = TemperatureStatisticsRESTInterface(_flask_args=frequest.args)
        _sensor_location = _temperature_stats_input.sensor_location
        _the_date = _temperature_stats_input.the_date

        if datetime.now() < _the_date:
            return bean_jsonified(NotAvailableJsonBean())

        _daily_chronology = self.data_source.temperature_daily_chronology(
            sensor_location=_sensor_location, the_date=_the_date)

        _stats_24h = TemperatureStatistics(
            _min_temp=_daily_chronology.get_min_daily()[1],
            _min_at=_daily_chronology.get_min_daily()[0],
            _max_temp=_daily_chronology.get_max_daily()[1],
            _max_at=_daily_chronology.get_max_daily()[0],
            _avg_temp=_daily_chronology.get_avg_daily(),
            _timestamp_from=_the_date.replace(hour=0, minute=0, second=0, microsecond=0),
            _timestamp_to=_the_date.replace(hour=23, minute=59, second=59, microsecond=999999),
            _sensor_location=_sensor_location,
            _sensor_reference=_daily_chronology.the_sensor.reference) \
            if _daily_chronology.is_valid() else NotAvailableJsonBean()

        _stats_day = TemperatureStatistics(
            _min_temp=_daily_chronology.get_min_day()[1],
            _min_at=_daily_chronology.get_min_day()[0],
            _max_temp=_daily_chronology.get_max_day()[1],
            _max_at=_daily_chronology.get_max_day()[0],
            _avg_temp=_daily_chronology.get_avg_day(),
            _timestamp_from=_daily_chronology.sun_set_rise.sunrise(),
            _timestamp_to=_daily_chronology.sun_set_rise.sunset(),
            _sensor_location=_sensor_location,
            _sensor_reference=_daily_chronology.the_sensor.reference) \
            if _daily_chronology.has_day_data() else NotAvailableJsonBean()

        _stats_night = TemperatureStatistics(
            _min_temp=_daily_chronology.get_min_night()[1],
            _min_at=_daily_chronology.get_min_night()[0],
            _max_temp=_daily_chronology.get_max_night()[1],
            _max_at=_daily_chronology.get_max_night()[0],
            _avg_temp=_daily_chronology.get_avg_night(),
            _timestamp_from=_the_date.replace(hour=0, minute=0, second=0, microsecond=0),
            _timestamp_to=_the_date.replace(hour=23, minute=59, second=59, microsecond=999999),
            _sensor_location=_sensor_location,
            _sensor_reference=_daily_chronology.the_sensor.reference) \
            if _daily_chronology.has_night_data() else NotAvailableJsonBean()

        return bean_jsonified(TemperatureDailyStatistics(
            _stats=_stats_24h,
            _stats_night=_stats_night,
            _stats_day=_stats_day,
            _date=_the_date,
            _location=_sensor_location,
            _reference=_daily_chronology.the_sensor.reference))

    @staticmethod
    def _datetime_fmt(to_convert: str):
        return datetime.strptime(to_convert, '%Y-%m-%d')

    def graph_temperature(self):
        _temp_graph_interface = TemperatureGraphRESTInterface(_flask_args=frequest.args)
        _sensor_location = _temp_graph_interface.sensor_location
        _the_date = _temp_graph_interface.the_date
        _title = _temp_graph_interface.graph_title
        _style = _temp_graph_interface.style

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
            the_date=_the_date,
            style=_style
        )

        return Response(_graph.plot_to_svg(), mimetype='image/svg+xml')

    def horizontal_progress_bar(self):
        progress_info = ProgressBarRESTInterface(_flask_args=frequest.args)
        return Response(
            a_graph.ProgressBar.cached_svg(
                progress=progress_info.progress,
                size=progress_info.size,
                do_show_border=progress_info.do_show_border,
                color_name=progress_info.color,
                colormap_name=progress_info.colormap
            ),
            mimetype='image/svg+xml')

    def graph_cesspit_last24h(self):
        cesspit_config = self.remote_cesspit_config.current_value()

        return Response(
            a_graph.Last24hCesspitGraph(
                data_source=self.data_source,
                tank_full_mm=cesspit_config.full_level_mm if cesspit_config.has_succeeded() else None,
                tank_empty_mm=cesspit_config.empty_level_mm if cesspit_config.has_succeeded() else None
            ).plot_to_svg(),
            mimetype='image/svg+xml')

    def graph_cesspit_single_day(self):
        cesspit_history_params = CesspitHistoryRESTInterface(_flask_args=frequest.args)
        cesspit_config = self.remote_cesspit_config.current_value()

        return Response(
            a_graph.DailyCesspitGraph(
                data_source=self.data_source,
                the_date=cesspit_history_params.the_date,
                tank_full_mm=cesspit_config.full_level_mm if cesspit_config.has_succeeded() else None,
                tank_empty_mm=cesspit_config.empty_level_mm if cesspit_config.has_succeeded() else None
            ).plot_to_svg(),
            mimetype='image/svg+xml')

    def graph_cesspit_per_day(self):
        cesspit_history_params = CesspitHistoryRESTInterface(_flask_args=frequest.args)
        cesspit_config = self.remote_cesspit_config.current_value()

        return Response(
            a_graph.PerDayCesspitGraph(
                data_source=self.data_source,
                the_date=cesspit_history_params.the_date,
                days_in_the_past=cesspit_history_params.days_in_past,
                tank_full_mm=cesspit_config.full_level_mm if cesspit_config.has_succeeded() else None,
                tank_empty_mm=cesspit_config.empty_level_mm if cesspit_config.has_succeeded() else None
            ).plot_to_svg(),
            mimetype='image/svg+xml')

    def graph_cesspit_prediction(self):
        cesspit_config = self.remote_cesspit_config.current_value()

        return Response(
            a_graph.PredictionCesspitGraph(
                data_source=self.data_source,
                tank_full_mm=cesspit_config.full_level_mm if cesspit_config.has_succeeded() else None,
                tank_empty_mm=cesspit_config.empty_level_mm if cesspit_config.has_succeeded() else None
            ).plot_to_svg(),
            mimetype='image/svg+xml')

    def cesspit_prediction(self):
        cesspit_config = self.remote_cesspit_config.current_value()
        _prediction = self.data_source.cesspit_prediction(
            tank_full_mm=cesspit_config.full_level_mm if cesspit_config.has_succeeded() else None,
            tank_empty_mm=cesspit_config.empty_level_mm if cesspit_config.has_succeeded() else None
        )
        return bean_jsonified(
            CesspitPredictionJson(
                predicted_date=_prediction.get_predicted_date(),
                as_of_date=_prediction.as_of_date
            )
        )

    def cesspit_log(self):
        return self.remote_cesspit_log.reroute()

    def _stop_system_status_thread(self):
        ExitEvent().set()
        self.system_status_thread.join()

    def system_status(self):
        return bean_jsonified(self.system_status_thread.reading())


# logging config
if sys.gettrace() is None:
    dictConfig({
        'version': 1,
        'formatters': {
            'default': {
                'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
            }
        },
        'handlers': {
            'wsgi': {
                'class': 'logging.StreamHandler',
                'formatter': 'default'
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'default',
                'filename': '/var/log/bhs/rest-info.log',
                'level': 'INFO'
            }
        },
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi', 'file']
        }
    })


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


@app.route('/current/solar_plant')
def current_solar_plant():
    return app.current_solar_power_production()


@app.route('/graph/temperature')
def graph_temperature():
    return app.graph_temperature()


@app.route('/graph/progress')
def graph_progress():
    return app.horizontal_progress_bar()


@app.route('/graph/cesspit/24h')
def graph_cesspit_last24h():
    return app.graph_cesspit_last24h()


@app.route('/graph/cesspit/daily')
def graph_cesspit_single_day():
    return app.graph_cesspit_single_day()


@app.route('/graph/cesspit/perday')
def graph_cesspit_per_day():
    return app.graph_cesspit_per_day()


@app.route('/graph/cesspit/prediction')
def graph_cesspit_prediction():
    return app.graph_cesspit_prediction()


@app.route('/current/wind')
def current_wind():
    return app.current_wind()


@app.route('/current/precipitation')
def current_precipitation():
    return app.current_precipitation()


@app.route('/history/temperature-daily')
def statistics_temperature():
    return app.statistics_temperature()


@app.route('/current/water-tank')
def current_water_tank():
    return app.current_water_tank()


@app.route('/current/cesspit-prediction')
def current_cesspit_prediction():
    return app.cesspit_prediction()


@app.route('/current/cesspit-log')
def current_cesspit_log():
    return app.cesspit_log()


@app.route('/system-status')
def system_status():
    return app.system_status()


if __name__ == '__main__':
    # in debug mode start the server using flask built-in server
    # of course for production use mod-wsgi_express on Apache2
    from werkzeug.serving import make_server

    # start on different port (production is 12000 to avoid conflicts)
    server = make_server('0.0.0.0', 12999, app)
    context = app.app_context()
    context.push()
    server.serve_forever()
