"""
The file collects routines used to prepare matplotlib graphs
"""
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.dates import DateFormatter, HourLocator, DateLocator
from matplotlib import ticker
from matplotlib import cm
from matplotlib import colors

from math import ceil
import io

from analysis.data import *
from core.bean import TemperatureGraphRESTInterface


class Graph:
    def __init__(self, size=None):
        mpl.rcParams["figure.dpi"] = 200
        self.figure, self.axes = plt.subplots(figsize=size, clear=True)

    def __del__(self):
        plt.close(self.figure)

    @staticmethod
    def _empty_svg():
        return f'<svg ' \
               f'width="1pt" height="1pt" viewBox="0 0 1 1" xmlns="http://www.w3.org/2000/svg" version="1.1">' \
               f'</svg>'

    def prepare_plot(self):
        raise NotImplementedError()

    def plot_to_svg(self) -> bytes:
        if not self.has_valid_data():
            return bytes(Graph._empty_svg(), encoding="UTF-8")

        self.prepare_plot()
        res = io.BytesIO()
        plt.savefig(res, format='svg', bbox_inches='tight', pad_inches=0, transparent=True)
        return res.getvalue()

    def has_valid_data(self):
        return NotImplementedError()


class DataGraph(Graph):
    def __init__(self, data_source: AnalysisDataSource):
        Graph.__init__(self)
        self.data_source = data_source

    def prepare_plot(self):
        raise NotImplementedError()


class DailyTemperatureGraph(DataGraph):

    def __init__(self,
                 data_source: AnalysisDataSource,
                 sensor_loc: str,
                 title: str,
                 style: str,
                 last_temp: tuple = None,
                 the_date: datetime = None):
        DataGraph.__init__(self, data_source)
        self.the_date = the_date if the_date else datetime.now()
        self.data = data_source.temperature_daily_chronology(sensor_location=sensor_loc, the_date=self.the_date)
        self.last_temp = last_temp if last_temp else self.data.get_perhour_last() if self.data.is_valid() else None
        self.style = style
        self.title = title

    def prepare_plot(self):
        if self.style == TemperatureGraphRESTInterface.STYLE_ERRORBARS:
            self.axes.errorbar(self.data.get_perhour_timeline(),
                               self.data.get_perhour_temperatures(),
                               yerr=[self.data.get_lower_errors(), self.data.get_upper_errors()],
                               linestyle='solid',
                               linewidth=2,
                               elinewidth=1, ecolor='orange', capsize=5)
        elif self.style == TemperatureGraphRESTInterface.STYLE_FILLBETWEEN:
            self.axes.fill_between(self.data.get_perhour_timeline(),
                                   self.data.get_perhour_min_temperatures(),
                                   self.data.get_perhour_max_temperatures(),
                                   color=cm.get_cmap(name='YlOrRd')(0.25))
            self.axes.plot(self.data.get_perhour_timeline(),
                           self.data.get_perhour_temperatures(),
                           linewidth=1)
        else:
            self.axes.plot(self.data.get_perhour_timeline(),
                           self.data.get_perhour_temperatures(),
                           linewidth=2)

        if self.last_temp:
            self.axes.scatter(self.last_temp[0], self.last_temp[1], marker='h')
            self.axes.annotate(f'  {self.last_temp[1]:.1f} \u2103', self.last_temp)

        _min = self.data.get_min_daily()
        self.axes.scatter(_min[0], _min[1])
        self.axes.annotate(f'  {_min[1]:.1f} \u2103', _min)
        _max = self.data.get_max_daily()
        self.axes.scatter(_max[0], _max[1])
        self.axes.annotate(f'  {_max[1]:.1f} \u2103', _max)

        self.axes.set_title(self.title)
        self.axes.set_ylabel('\u2103')
        self.axes.set_xlim(datetime(self.the_date.year, self.the_date.month, self.the_date.day, 0, 0, 0),
                           datetime(self.the_date.year, self.the_date.month, self.the_date.day, 0, 0, 0) + timedelta(days=1))
        self.axes.xaxis_date()
        self.axes.xaxis.set_major_formatter(DateFormatter('%H'))
        self.axes.xaxis.set_tick_params(labelrotation=90)
        self.axes.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:.0f}'))

    def has_valid_data(self):
        return self.data.is_valid()


class ProgressBar(Graph):
    BACKGROUND = cm.get_cmap(name='Greys')(0.3)

    def __init__(self, progress: int, size: tuple, do_show_border=False, colormap_name='BuGn', color_name=None):
        Graph.__init__(self, size)
        self.progress = progress
        self.show_border = do_show_border
        self.color = colors.to_rgba(colors.get_named_colors_mapping().get(color_name)) \
            if color_name is not None else cm.get_cmap(name=colormap_name)(self.progress / 100)

    def prepare_plot(self):
        bar_remaining = self.axes.barh('.', 100-self.progress, left=self.progress, color=self.BACKGROUND)
        bar_progress = self.axes.barh('.', self.progress, color=self.color)
        self.axes.set(xlim=(0, 100))
        is_label_outside = self.progress < 30
        font_color = 'black' if is_label_outside else 'black' if sum(self.color[:3]) > 1.2 else 'white'
        self.axes.bar_label(bar_progress, padding=3, label_type='edge' if is_label_outside else 'center',
                            fmt='%g %%', fontsize='medium', fontfamily='Lato', color=font_color)
        self.axes.yaxis.set_ticklabels([])
        self.axes.xaxis.set_ticklabels([])
        self.axes.set_yticks([])
        self.axes.set_xticks([])
        if not self.show_border:
            self.axes.set_axis_off()


class BaseCesspitGraph(DataGraph):
    LABEL_FILL_PERC = f'Wypełnienie [%]'
    LABEL_DELTA = f'\u0394 [%]'

    def __init__(self, data_source: AnalysisDataSource):
        """
        Constructs the object responsible for plotting the graph of Cesspit usage
        :param data_source: the AnalysisDataSource, which will be used to query the data
        """
        DataGraph.__init__(self, data_source)
        self.bar_color_map = cm.get_cmap(name='RdYlGn_r')
        self.continuous_line_color_map = cm.get_cmap(name='OrRd_r')
        self.estimated_max_hourly_gain_perc = 2.0
        self.estimated_avg_hourly_gain_perc = 0.1
        self.estimated_max_daily_gain_perc = 10.0
        self.estimated_avg_daily_gain_perc = 5.0

    @staticmethod
    def smart_fill_range(fill_perc_continuous: list) -> tuple:
        _fr = (
            10*(min([min(_fp) for _fp in fill_perc_continuous]) // 10),
            10*(ceil(max([max(_fp) for _fp in fill_perc_continuous]) / 10))
        )
        return _fr[0], _fr[1] if _fr[1] < 100 else 105

    @staticmethod
    def smart_delta_range(deltas_per_hour, est_max) -> tuple:
        return (
            0,
            max(est_max, ceil(max(deltas_per_hour)))
        )

    def continous_plot_colors(self, count: int) -> list:
        return [self.continuous_line_color_map(_c_id / (count + 1)) for _c_id in range(1, count + 1)]

    def hourly_bar_color(self, _delta: float):
        """
        Having provided delta value, a color of the bar showing increase is returned
        :param _delta:
        :return:
        """
        return self.bar_color_map(
            (_delta - self.estimated_avg_hourly_gain_perc) /
            (self.estimated_max_hourly_gain_perc - self.estimated_avg_hourly_gain_perc)
        )

    def daily_bar_color(self, _delta: float):
        """
        Having provided delta value, a color of the bar showing increase is returned
        :param _delta:
        :return:
        """
        return self.bar_color_map(
            (_delta - self.estimated_avg_daily_gain_perc) /
            (self.estimated_max_daily_gain_perc - self.estimated_avg_daily_gain_perc)
        )


class PerHourCesspitGraph(BaseCesspitGraph):

    def __init__(
            self,
            data_source: AnalysisDataSource,
            the_date: datetime,
            hours_in_the_past: int,
            extend_to_nearest_midnight: bool,
            extend_with_adherent_records: bool,
            tank_full_mm: int, tank_empty_mm: int):
        """
        Initializes the base class for all graphs that shows cesspit observations gathered per hour.
        :param data_source: the object that will be used to query for data
        :param the_date: the ending date of the observed period
        :param hours_in_the_past: number of hours in the past
        :param extend_to_nearest_midnight: if True, the x-axis will be extended till end of the day
        :param extend_with_adherent_records: if True, the records adherent to given period in past and future
        will be also considered
        :param tank_full_mm: configuration entry: the level of tank being full
        :param tank_empty_mm: configuration entry: the level of tank being empty
        """
        BaseCesspitGraph.__init__(self, data_source)
        self.extend_to_nearest_midnight = extend_to_nearest_midnight
        self.data = data_source.cesspit_increase(
            the_date=the_date,
            hours_in_past=hours_in_the_past,
            add_adherent_records=extend_with_adherent_records,
            tank_full_mm=tank_full_mm,
            tank_empty_mm=tank_empty_mm
        )

    def prepare_plot(self):
        """
        The graph is composed of two plots:
        - bars per each hour, showing level increase at certain hour
        - continuous line showing the level
        :return: None, just prepares the plot
        """
        _axis_continuous = self.axes
        _axis_continuous.xaxis.set_tick_params(labelrotation=90)
        _axis_continuous.xaxis.set_major_formatter(DateFormatter('%H'))
        _axis_continuous.xaxis.set_minor_locator(HourLocator())
        _axis_daily = self.axes.twinx()
        _axis_daily.xaxis.set_tick_params(labelrotation=90)
        _axis_daily.xaxis.set_major_formatter(DateFormatter('%H'))
        _axis_daily.xaxis.set_minor_locator(HourLocator())

        # vertical bars for per-hour increase
        _timeline_per_hour = self.data.get_per_hour_timeline()
        _deltas_per_hour = self.data.get_per_hour_deltas_perc()
        _bars = _axis_daily.bar(_timeline_per_hour, _deltas_per_hour, width=timedelta(minutes=40),
                                color=[self.hourly_bar_color(_d) for _d in _deltas_per_hour])
        _axis_daily.bar_label(_bars, ['' if _d < 0.1 else f'+{_d:.1f}%' for _d in _deltas_per_hour], rotation=90)

        # continuous line for the actual state
        _timeline_continuous = self.data.get_continuous_timeline()
        _fill_perc_continuous = self.data.get_continuous_fill_perc()

        for _tm, _lv, _col in zip(
                _timeline_continuous,
                _fill_perc_continuous,
                self.continous_plot_colors(len(_timeline_continuous))
        ):
            _axis_continuous.plot(_tm, _lv, color=_col, linewidth=1.5)

        _axis_continuous.set_ylim(*BaseCesspitGraph.smart_fill_range(_fill_perc_continuous))
        _axis_continuous.set_ylabel(BaseCesspitGraph.LABEL_FILL_PERC)
        _axis_daily.set_ylim(*BaseCesspitGraph.smart_delta_range(_deltas_per_hour, self.estimated_max_hourly_gain_perc))
        _axis_daily.set_ylabel(BaseCesspitGraph.LABEL_DELTA)
        if self.extend_to_nearest_midnight:
            self.axes.set_xlim(
                min(_timeline_continuous[0]),
                max(_timeline_continuous[-1]).replace(hour=23, minute=59, second=59)
            )
        else:
            self.axes.set_xlim(
                min(_timeline_continuous[0]),
                max(_timeline_continuous[-1])
            )

        if _axis_continuous.get_ylim()[1] > 100:
            self.axes.axhline(100, color='red', dashes=(5, 2), linewidth=0.5)

    def has_valid_data(self):
        return self.data.is_valid()


class Last24hCesspitGraph(PerHourCesspitGraph):

    def __init__(self, data_source: AnalysisDataSource, tank_full_mm: int, tank_empty_mm: int):
        """
        Constructs the object responsible for plotting the graph of Cesspit usage during last 24h
        :param data_source: the AnalysisDataSource, which will be used to query the data
        :param tank_full_mm: the configuration parameter: level of tank being full
        :param tank_empty_mm: the configuration parameter: level of tank being empty
        """
        PerHourCesspitGraph.__init__(
            self, data_source=data_source,
            the_date=datetime.now(),
            hours_in_the_past=23,
            extend_to_nearest_midnight=False,
            extend_with_adherent_records=False,
            tank_full_mm=tank_full_mm, tank_empty_mm=tank_empty_mm
        )


class DailyCesspitGraph(PerHourCesspitGraph):

    def __init__(self, data_source: AnalysisDataSource, the_date: datetime, tank_full_mm: int, tank_empty_mm: int):
        """
        Constructs the object responsible for plotting the graph of Cesspit usage during given day
        :param data_source: the AnalysisDataSource, which will be used to query the data
        :param the_date
        :param tank_full_mm: the configuration parameter: level of tank being full
        :param tank_empty_mm: the configuration parameter: level of tank being empty
        """
        _now = datetime.now()
        _for_today = the_date.year == _now.year \
                     and the_date.month == _now.month \
                     and the_date.day == _now.day
        PerHourCesspitGraph.__init__(
            self, data_source=data_source,
            the_date=the_date.replace(hour=23, minute=59, second=59),
            hours_in_the_past=23,
            extend_to_nearest_midnight=_for_today,
            extend_with_adherent_records=True,
            tank_full_mm=tank_full_mm, tank_empty_mm=tank_empty_mm
        )


class PerDayCesspitGraph(BaseCesspitGraph):

    def __init__(
            self,
            data_source: AnalysisDataSource,
            the_date: datetime,
            days_in_the_past: int,
            tank_full_mm: int, tank_empty_mm: int):
        """
        Initializes the base class for all graphs that shows cesspit observations gathered per hour.
        :param data_source: the object that will be used to query for data
        :param the_date: the ending date of the observed period
        :param days_in_the_past: number of days in the past
        :param tank_full_mm: configuration entry: the level of tank being full
        :param tank_empty_mm: configuration entry: the level of tank being empty
        """
        BaseCesspitGraph.__init__(self, data_source)
        _now = datetime.now()
        self.data = data_source.cesspit_increase(
            the_date=the_date,
            days_in_past=days_in_the_past,
            tank_full_mm=tank_full_mm,
            tank_empty_mm=tank_empty_mm
        )

    def prepare_plot(self):
        """
        The graph is composed of two plots:
        - bars per each day, showing level increase at certain day
        - continuous line showing the level
        :return: None, just prepares the plot
        """
        _axis_continuous = self.axes
        _axis_continuous.xaxis.set_tick_params(labelrotation=90)
        _axis_continuous.xaxis.set_major_formatter(DateFormatter('%b %d'))
        # _axis_continuous.xaxis.set_minor_locator(DateLocator())
        _axis_daily = self.axes.twinx()
        _axis_daily.xaxis.set_tick_params(labelrotation=90)
        _axis_daily.xaxis.set_major_formatter(DateFormatter('%b %d'))
        # _axis_daily.xaxis.set_minor_locator(DateLocator())

        # vertical bars for per-hour increase
        _timeline_per_day = self.data.get_per_day_timeline()
        _deltas_per_day = self.data.get_per_day_deltas_perc()
        _bars = _axis_daily.bar(_timeline_per_day, _deltas_per_day, width=timedelta(hours=6),
                                color=[self.daily_bar_color(_d) for _d in _deltas_per_day])
        _axis_daily.bar_label(_bars, ['' if _d < 0.1 else f'+{_d:.1f}%' for _d in _deltas_per_day], rotation=90)

        # continuous line for the actual state
        _timeline_continuous = self.data.get_continuous_timeline()
        _fill_perc_continuous = self.data.get_continuous_fill_perc()

        for _tm, _lv, _col in zip(
                _timeline_continuous,
                _fill_perc_continuous,
                self.continous_plot_colors(len(_timeline_continuous))
        ):
            _axis_continuous.plot(_tm, _lv, color=_col, linewidth=1.5)

        _axis_continuous.set_ylim(*BaseCesspitGraph.smart_fill_range(_fill_perc_continuous))
        _axis_continuous.set_ylabel(BaseCesspitGraph.LABEL_FILL_PERC)
        _axis_daily.set_ylim(*BaseCesspitGraph.smart_delta_range(_deltas_per_day, self.estimated_max_daily_gain_perc))
        _axis_daily.set_ylabel(BaseCesspitGraph.LABEL_DELTA)
        self.axes.set_xlim(
            min(_timeline_continuous[0]),
            max(_timeline_continuous[-1])
        )

        if _axis_continuous.get_ylim()[1] > 100:
            self.axes.axhline(100, color='red', dashes=(5, 2), linewidth=0.5)


if __name__ == '__main__':
    data_source = analysis_data_source()
    # svg = DailyTemperatureGraph(data_source=analysis_data_source(), sensor_loc='External', title='Temperatura zewnętrzna').plot_to_svg()
    # svg = ProgressBar(progress=20).plot_to_svg()
    # svg = Last24hCesspitGraph(data_source=data_source, tank_full_mm=500, tank_empty_mm=1952).plot_to_svg()
    svg = DailyCesspitGraph(data_source=data_source, the_date=datetime.now(), tank_full_mm=500, tank_empty_mm=1952).plot_to_svg()
    # svg = DailyCesspitGraph(data_source=data_source, the_date=datetime(2023, 10, 21), tank_full_mm=500, tank_empty_mm=1952).plot_to_svg()
    # svg = DailyCesspitGraph(data_source=data_source, the_date=datetime(2023, 10, 27), tank_full_mm=500, tank_empty_mm=1952).plot_to_svg()
    # svg = PerDayCesspitGraph(data_source=data_source, the_date=datetime(2023, 11, 3), days_in_the_past=28, tank_full_mm=500, tank_empty_mm=1952).plot_to_svg()

    print(svg)
