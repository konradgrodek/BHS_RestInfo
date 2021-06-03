"""
The file collects routines used to prepare matplotlib graphs
"""
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib import ticker
from datetime import timedelta
import io

from analysis.data import *


class Graph:
    def __init__(self, data_source: AnalysisDataSource):
        self.data_source = data_source
        self.figure, self.axis = plt.subplots()

    def prepare_plot(self):
        raise NotImplementedError()

    def plot_to_svg(self) -> bytes:
        self.prepare_plot()
        res = io.BytesIO()
        plt.savefig(res, format='svg')
        return res.getvalue()


class DailyTemperatureGraph(Graph):

    def __init__(self,
                 data_source: AnalysisDataSource,
                 sensor_loc: str,
                 title: str,
                 last_temp: tuple = None,
                 the_date: datetime = datetime.now()):
        Graph.__init__(self, data_source)
        self.the_date = the_date if the_date else datetime.now()
        self.data = data_source.temperature_daily_chronology(sensor_location=sensor_loc, the_date=self.the_date)
        self.last_temp = last_temp if last_temp else self.data.get_perhour_last() if the_date else None
        self.title = title

    def prepare_plot(self):
        self.axis.errorbar(self.data.get_perhour_timeline(),
                           self.data.get_perhour_temperatures(),
                           yerr=[self.data.get_lower_errors(), self.data.get_upper_errors()],
                           linestyle='solid',
                           linewidth=2,
                           elinewidth=1, ecolor='orange', capsize=5)

        if self.last_temp:
            self.axis.scatter(self.last_temp[0], self.last_temp[1], marker='h')
            self.axis.annotate(f'  {self.last_temp[1]:.1f} \u2103', self.last_temp)

        _min = self.data.get_min()
        self.axis.scatter(_min[0], _min[1])
        self.axis.annotate(f'  {_min[1]:.1f} \u2103', _min)
        _max = self.data.get_max()
        self.axis.scatter(_max[0], _max[1])
        self.axis.annotate(f'  {_max[1]:.1f} \u2103', _max)

        self.axis.set_title(self.title)
        self.axis.set_ylabel('\u2103')
        self.axis.set_xlim(datetime(self.the_date.year, self.the_date.month, self.the_date.day, 0, 0, 0),
                           datetime(self.the_date.year, self.the_date.month, self.the_date.day, 0, 0, 0) + timedelta(days=1))
        self.axis.xaxis_date()
        self.axis.xaxis.set_major_formatter(DateFormatter('%H'))
        self.axis.xaxis.set_tick_params(labelrotation=90)
        self.axis.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:.0f}'))


if __name__ == '__main__':
    svg = DailyTemperatureGraph(data_source=analysis_data_source(), sensor_loc='External', title='Temperatura zewnętrzna').plot_to_svg()

    print(svg)