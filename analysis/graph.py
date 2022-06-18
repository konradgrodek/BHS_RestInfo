"""
The file collects routines used to prepare matplotlib graphs
"""
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib import ticker
from matplotlib import cm
from datetime import timedelta
import io

from analysis.data import *


class Graph:
    def __init__(self, size=None):
        self.figure, self.axes = plt.subplots(figsize=size)

    def prepare_plot(self):
        raise NotImplementedError()

    def plot_to_svg(self) -> bytes:
        self.prepare_plot()
        res = io.BytesIO()
        plt.savefig(res, format='svg')
        return res.getvalue()


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
                 last_temp: tuple = None,
                 the_date: datetime = datetime.now()):
        DataGraph.__init__(self, data_source)
        self.the_date = the_date if the_date else datetime.now()
        self.data = data_source.temperature_daily_chronology(sensor_location=sensor_loc, the_date=self.the_date)
        self.last_temp = last_temp if last_temp else self.data.get_perhour_last() if the_date else None
        self.title = title

    def prepare_plot(self):
        self.axes.errorbar(self.data.get_perhour_timeline(),
                           self.data.get_perhour_temperatures(),
                           yerr=[self.data.get_lower_errors(), self.data.get_upper_errors()],
                           linestyle='solid',
                           linewidth=2,
                           elinewidth=1, ecolor='orange', capsize=5)

        if self.last_temp:
            self.axes.scatter(self.last_temp[0], self.last_temp[1], marker='h')
            self.axes.annotate(f'  {self.last_temp[1]:.1f} \u2103', self.last_temp)

        _min = self.data.get_min()
        self.axes.scatter(_min[0], _min[1])
        self.axes.annotate(f'  {_min[1]:.1f} \u2103', _min)
        _max = self.data.get_max()
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


class ProgressBar(Graph):
    BACKGROUND = cm.get_cmap(name='Greys')(0.1)

    def __init__(self, progress: int, size: tuple, do_show_border=False, colormap_name='BuGn'):
        Graph.__init__(self, size)
        self.progress = progress
        self.show_border = do_show_border
        self.colormap = cm.get_cmap(name=colormap_name)

    def prepare_plot(self):
        bar_remaining = self.axes.barh('.', 100-self.progress, left=self.progress, color=self.BACKGROUND)
        bar_progress = self.axes.barh('.', self.progress, color=self.colormap(self.progress/100))
        self.axes.set(xlim=(0, 100))
        self.axes.bar_label(bar_progress, padding=3)
        self.axes.yaxis.set_ticklabels([])
        self.axes.xaxis.set_ticklabels([])
        self.axes.set_yticks([])
        self.axes.set_xticks([])
        if not self.show_border:
            self.axes.set_axis_off()


if __name__ == '__main__':
    # svg = DailyTemperatureGraph(data_source=analysis_data_source(), sensor_loc='External', title='Temperatura zewnętrzna').plot_to_svg()
    svg = ProgressBar(progress=20).plot_to_svg()

    print(svg)
