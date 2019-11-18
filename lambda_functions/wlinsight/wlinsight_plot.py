import io
import logging
from datetime import datetime
from textwrap import wrap

import matplotlib.dates as mdates
from matplotlib.figure import Figure
import pandas as pd
from lambda_proxy.proxy import API
from matplotlib.patches import Rectangle
from pandas.plotting import register_matplotlib_converters

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


register_matplotlib_converters()

APP = API(name="app")


@APP.route("/plot/<poly_id>", methods=["GET"], cors=True, binary_b64encode=True)
def print_id(poly_id):
    LOG.info("Generating plot for polygon id: %s", poly_id)

    data = load_wetland_data("area_percent_0_10245.csv")
    png = plot_to_png(data, "Sample name")

    return ("OK", "image/png", png)


def load_wetland_data(file_name):
    data = pd.read_csv(file_name, parse_dates=["time"])
    return data


def plot_to_png(count, wetland_name):
    pal = ["#0047AB", "#6347FF", "#7cfc00", "#f5f5dc", "#964B00"]
    fig = Figure(figsize=(22, 6))
    ax = fig.subplots()
    ax.stackplot(
        count.time.values,
        count.water,
        count.TCW,
        count.PV,
        count.NPV,
        count.BS,
        labels=["open water", "wet", "green veg", "dry veg", "bare soil",],
        colors=pal,
        alpha=0.6,
    )
    # set axis limits to the min and max
    ax.axis(xmin=count.time.values[0], xmax=count.time.values[-1], ymin=0, ymax=100)
    # add a legend and a tight plot box
    ax.legend(loc="lower left", framealpha=0.6)
    # ax.tight_layout()
    years = mdates.YearLocator(1)
    years_format = mdates.DateFormatter("%Y")
    # ax = plt.gca()
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(years_format)
    # ax.yaxis.set_ticks(np.arange(0,110,10))
    ax.set_xlabel(
        f"The Fractional Cover algorithm developed by the Joint Remote"
        f" Sensing Research Program and \n the Water Observations from Space algorithm "
        f"developed by Geoscience Australia are used in the production of this data",
        style="italic",
    )
    ls58_gap_start = datetime(2011, 11, 1)
    ls_58_gap_end = datetime(2013, 4, 1)

    # convert to matplotlib date representation
    gap_start = mdates.date2num(ls58_gap_start)
    gap_end = mdates.date2num(ls_58_gap_end)
    gap = gap_end - gap_start

    # set up rectangle
    slc_rectangle = Rectangle(
        (gap_start, 0),
        gap,
        100,
        alpha=0.5,
        facecolor="#ffffff",
        edgecolor="#ffffff",
        hatch="////",
        linewidth=2,
    )
    ax.add_patch(slc_rectangle)

    # this section wraps text for polygon names that are too long
    wetland_name = wetland_name.replace("'", "\\'")
    title = ax.set_title(
        "\n".join(
            wrap(
                f"Percentage of area dominated by WOfS, Wetness, Fractional Cover for {wetland_name}"
            )
        )
    )
    fig.tight_layout()
    title.set_y(1.05)

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    contents = buf.read()
    return contents
