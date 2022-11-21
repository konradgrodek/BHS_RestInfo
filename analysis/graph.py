"""
The file collects routines used to prepare matplotlib graphs
"""
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, HourLocator
from matplotlib import ticker
from matplotlib import cm
from matplotlib import colors
import io

from analysis.data import *
from core.bean import TemperatureGraphRESTInterface


class Graph:
    def __init__(self, size=None):
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


class Last24hCesspitGraph(DataGraph):

    def __init__(self, data_source: AnalysisDataSource, tank_full_mm: int, tank_empty_mm: int):
        """
        Constructs the object responsible for plotting the graph of Cesspit usage during last 24h
        :param data_source: the AnalysisDataSource, which will be used to query the data
        :param tank_full_mm: the configuration parameter: level of tank being full
        :param tank_empty_mm: the configuration parameter: level of tank being empty
        """
        DataGraph.__init__(self, data_source)
        self.data = data_source.cesspit_increase(
            the_date=datetime.now(), hours_in_past=24)
        self.tank_full_mm = tank_full_mm
        self.tank_empty_mm = tank_empty_mm
        self.bar_color_map = cm.get_cmap(name='RdYlGn_r')
        self.estimated_max_hourly_gain_perc = 2.0
        self.estimated_avg_hourly_gain_perc = 0.1

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
        _deltas_per_hour = self.data.get_per_hour_deltas_perc(
            tank_full_mm=self.tank_full_mm, tank_empty_mm=self.tank_empty_mm)
        _bars = _axis_daily.bar(_timeline_per_hour, _deltas_per_hour, width=0.02,
                                color=[self._bar_color(_d) for _d in _deltas_per_hour])
        _axis_daily.bar_label(_bars, ['' if _d < 0.1 else f'+{_d:.1f}%' for _d in _deltas_per_hour], rotation=90)

        # continuous line for the actual state
        _timeline_continuous = self.data.get_continuous_timeline()
        _fill_perc_continuous = self.data.get_continuous_fill_perc(
            tank_full_mm=self.tank_full_mm, tank_empty_mm=self.tank_empty_mm)

        for _tm, _lv in zip(_timeline_continuous, _fill_perc_continuous):
            _axis_continuous.plot(_tm, _lv, color='red', linewidth=2)

        _axis_continuous.set_ylim(0, 100)
        _axis_continuous.set_ylabel(f'Wypełnienie [%]')
        _axis_daily.set_ylim(0, max(self.estimated_max_hourly_gain_perc, max(_deltas_per_hour)))
        _axis_daily.set_ylabel(f'\u0394 [%]')

    def _bar_color(self, _delta: float):
        """
        Having provided delta value, a color of the bar showing increase is returned
        :param _delta:
        :return:
        """
        return self.bar_color_map(
            (_delta - self.estimated_avg_hourly_gain_perc) /
            (self.estimated_max_hourly_gain_perc - self.estimated_avg_hourly_gain_perc)
        )

    def has_valid_data(self):
        return self.tank_full_mm is not None and self.tank_empty_mm is not None


if __name__ == '__main__':
    # svg = DailyTemperatureGraph(data_source=analysis_data_source(), sensor_loc='External', title='Temperatura zewnętrzna').plot_to_svg()
    svg = ProgressBar(progress=20).plot_to_svg()

    print(svg)
