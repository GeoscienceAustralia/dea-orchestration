import json
import logging
import boto3
import os
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from datetime import datetime
import numpy as np
from textwrap import wrap
import pandas as pd
import io

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def load_csv(file_name):
    d = pd.read_csv('area_percent_0_10245.csv')
    return d


def plot_to_png(count, polyName):   
    pal = ['#0047AB',
            '#6347FF',
            '#7cfc00',
            '#f5f5dc',
            '#964B00']
    fig = plt.figure(figsize = (22,6))
    plt.stackplot(count.time.values,
                    count.water,
                    count.TCW,
                    count.PV,
                    count.NPV,
                    count.BS,
                    labels=['open water',
                    'wet',
                    'green veg',
                    'dry veg',
                    'bare soil',
                    ], colors=pal, alpha = 0.6)
    #set axis limits to the min and max
    plt.axis(xmin = count.time[0].data, xmax = count.time[-1].data, ymin = 0, ymax = 100)
    #add a legend and a tight plot box
    plt.legend(loc='lower left', framealpha=0.6)
    plt.tight_layout()
    years = mdates.YearLocator(1)
    yearsFmt = mdates.DateFormatter('%Y')
    ax = plt.gca()
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    #ax.yaxis.set_ticks(np.arange(0,110,10))
    ax.set_xlabel(f'The Fractional Cover algorithm developed by the Joint Remote'
    f' Sensing Research Program and \n the Water Observations from Space algorithm '
    f'developed by Geoscience Australia are used in the production of this data',style='italic')
    LS5_8_gap_start = datetime(2011,11,1)
    LS5_8_gap_end = datetime(2013,4,1)

    # convert to matplotlib date representation
    gap_start = mdates.date2num(LS5_8_gap_start)
    gap_end = mdates.date2num(LS5_8_gap_end)
    gap = gap_end - gap_start

    # set up rectangle
    slc_rectangle= Rectangle((gap_start,0), gap, 100,alpha = 0.5, facecolor='#ffffff',
                edgecolor='#ffffff', hatch="////",linewidth=2)
    ax.add_patch(slc_rectangle)

    # this section wraps text for polygon names that are too long
    polyName=polyName.replace("'","\\'")
    title=ax.set_title("\n".join(wrap(f'Percentage of area dominated by WOfS, Wetness, Fractional Cover for {polyName}')))
    fig.tight_layout()
    title.set_y(1.05)
    plt.close()

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    contents = buf.read()
    return contents

from lambda_proxy.proxy import API

APP = API(name="app")

@APP.route('/test/tests/<filename>.jpg', methods=['GET'], cors=True, binary_b64encode=True)
def print_id(body):
    data = load_csv()
    png = plot_to_png(data, 'Sample name')

    return ('OK', 'image/png', png)