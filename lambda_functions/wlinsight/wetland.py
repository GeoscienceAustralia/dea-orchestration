from datacube import Datacube
from datacube.virtual.impl import VirtualDatasetBox
from datacube.virtual import construct
from datacube.utils.geometry import CRS, Geometry
from datacube_stats.utils.dates import date_sequence
from dask.distributed import LocalCluster, Client
import dask.array as da
import pandas as pd

from shapely.geometry import Polygon, MultiPolygon
import fiona
import yaml
import numpy as np

from rasterio import features
from rasterio.warp import calculate_default_transform
import xarray as xr

import sys
import os
from os import path
import seaborn as sns

from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from textwrap import wrap

import click
import copy
import pickle
def generate_raster(shapes, data, dst_crs, enable_dask):
    yt, xt = data.shape[1:]
    xres = 25
    yres = 25
    no_data = 0
    left = data.coords['x'].min().data - xres * 0.5
    bottom = data.coords['y'].min().data - yres * 0.5
    right = data.coords['x'].max().data + xres * 0.5
    top = data.coords['y'].max().data + yres*0.5
    print('image bound', (left, bottom, right, top))
    transform, _, _= calculate_default_transform(
            'EPSG:3577' , dst_crs, xt, yt, left, bottom, right, top)
    print('transform affine', transform)
    target_ds = features.rasterize(shapes, (yt, xt), transform=transform, all_touched=True)
    if enable_dask:
        return da.from_array(target_ds).rechunk(data.data.chunksize[1:])
    else:
        return target_ds

def aggregate_data(fc_results, water_results):
    j = 1
    tmp = {}
    for var in fc_results.data_vars:
        tmp[var] = fc_results[var][0].copy()
    tmp_water = water_results[0].copy()
    while j < fc_results.time.size:
        print('aggregate over', fc_results.time.data[j])
        for var in fc_results.data_vars:
            tmp[var] = (tmp[var].where(tmp[var] != fc_results[var].attrs['nodata'], 0)
                        + fc_results[var][j].where(tmp[var] == fc_results[var].attrs['nodata'], 0))
        tmp_water = np.logical_or(tmp_water.where(tmp['TCW'] != fc_results['TCW'].attrs['nodata'],
                                                False),
                        water_results[j].where(tmp['TCW'] == fc_results['TCW'].attrs['nodata'],
                                                False))
        j += 1
    tmp = xr.merge(tmp.values())

    return tmp, tmp_water

def aggregate_over_time(fc_results, water_results):
    aggregate_fc = None
    aggregate_water = None
    i = 0

    while i < len(fc_results.time):
        if i >= len(fc_results.time) - 1:
            aggregate_fc = xr.concat([aggregate_fc, fc_results.sel(time=fc_results.time.data[i])], dim='time')
            aggregate_water = xr.concat([aggregate_water, water_results.sel(time=fc_results.time.data[i])], dim='time')
            break
        j = 1
        tmp = {}
        for var in fc_results.data_vars:
            tmp[var] = fc_results[var][i].copy()
        tmp_water = water_results[i].copy()
        while np.abs(fc_results.time.data[i] - fc_results.time.data[i+j]).astype('timedelta64[D]') < np.timedelta64(15, 'D'):
            print('aggregate over', fc_results.time.data[i+j])
            for var in fc_results.data_vars:
                tmp[var] = (tmp[var].where(tmp[var] != fc_results[var].attrs['nodata'], 0)
                            + fc_results[var][i+j].where(tmp[var] == fc_results[var].attrs['nodata'], 0))
            tmp_water = np.logical_or(tmp_water.where(tmp['TCW'] != fc_results['TCW'].attrs['nodata'],
                                                    False),
                            water_results[i+j].where(tmp['TCW'] == fc_results['TCW'].attrs['nodata'],
                                                    False))
            j += 1
            if i + j >= len(fc_results.time):
                break
        i += j

        tmp = xr.merge(tmp.values())
        if aggregate_fc is None:
            aggregate_fc = tmp
        else:
            aggregate_fc = xr.concat([aggregate_fc, tmp], dim='time')

        if aggregate_water is None:
            aggregate_water = tmp_water
        else:
            aggregate_water = xr.concat([aggregate_water, tmp_water], dim='time')

    aggregate_fc.attrs = fc_results.attrs
    for var in fc_results.data_vars:
        aggregate_fc[var].attrs = fc_results[var].attrs
    aggregate_water.attrs = water_results.attrs

    return aggregate_fc, aggregate_water

def intersect_with_landsat(shape):
    landsat_shp = '/g/data1a/u46/users/ea6141/aus_map/landsat_au.shp'
    if shape['geometry']['type'] == 'MultiPolygon':
        pl_wetland = []
        for coords in shape['geometry']['coordinates']:
            pl_wetland.append(Polygon(coords[0]))
        pl_wetland = MultiPolygon(pl_wetland)
    else:
        pl_wetland = Polygon(shape['geometry']['coordinates'][0])

    contain = {}
    intersect = {}
    with fiona.open(landsat_shp) as shapes:
        for s in shapes:
            pl_landsat = Polygon(s['geometry']['coordinates'][0])
            if pl_wetland.within(pl_landsat):
                contain[s['id']] = True
            elif pl_wetland.intersects(pl_landsat):
                if (pl_wetland.intersection(pl_landsat).area / pl_wetland.area < 0.9):
                    intersect[s['id']] = True
                else:
                    intersect[s['id']] = False

    return contain, intersect

def calculate_area(aggregate_fc, aggregate_water, mask_array, true_valid=False):
    count = []
    for var in aggregate_fc.data_vars:
        valid_data = np.logical_and((aggregate_fc[var] != aggregate_fc[var].attrs['nodata']), (mask_array == 1))
        if var == 'TCW':
            if true_valid:
                valid_area_fc = aggregate_fc.TCW.where(valid_data).count(dim=['y', 'x'])
            else:
                valid_area_fc = None
            valid_data = np.logical_and(valid_data, (aggregate_fc.TCW >= -350))
            count.append(aggregate_fc.TCW.where(valid_data).count(dim=['y', 'x']))
        else:
            count.append(aggregate_fc[var].where(valid_data).sum(dim=['y', 'x'])/100.)

    valid_data = np.logical_and(aggregate_water, (mask_array == 1))
    water_count = aggregate_water.where(valid_data).count(dim=['y', 'x'])
    count.append(water_count)
    return xr.merge(count), valid_area_fc

def calculate_dom_area(aggregate_fc, aggregate_water, mask_array):
    count = []
    dom_value = aggregate_fc.to_array().max(dim='variable')
    dom_data = None
    for var in aggregate_fc.data_vars:
        valid_data = np.logical_and((aggregate_fc[var] != aggregate_fc[var].attrs['nodata']), (mask_array == 1))
        if var == 'TCW':
            valid_data = np.logical_and(valid_data, (aggregate_fc.TCW >= -350))
            count.append(aggregate_fc.TCW.where(valid_data).count(dim=['y', 'x']))
        else:
            valid_data = np.logical_and(valid_data, (aggregate_fc[var] == dom_value))
            if dom_data is None:
                dom_data = valid_data 
            else:
                dom_data = np.logical_and(valid_data, np.logical_not(dom_data))
            count.append(aggregate_fc[var].where(dom_data).count(dim=['y', 'x']))

    valid_data = np.logical_and(aggregate_water, (mask_array == 1))
    water_count = aggregate_water.where(valid_data).count(dim=['y', 'x'])
    count.append(water_count)
    return xr.merge(count), None

def plot_to_png(count, feature_id, polyName, output_location):
    pal = [sns.xkcd_rgb["cobalt blue"],
            sns.xkcd_rgb["neon blue"],
            sns.xkcd_rgb["grass"],
            sns.xkcd_rgb["beige"],
            sns.xkcd_rgb["brown"]]        
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
    slc_rectangle= Rectangle((gap_start,0), gap, 100,alpha = 0.5, facecolor=sns.xkcd_rgb['white'],
    edgecolor=sns.xkcd_rgb['white'], hatch="////",linewidth=2)
    ax.add_patch(slc_rectangle)

    # this section wraps text for polygon names that are too long
    polyName=polyName.replace("'","\\'")
    title=ax.set_title("\n".join(wrap(f'Percentage of area dominated by WOfS, Wetness, Fractional Cover for {polyName}')))
    fig.tight_layout()
    title.set_y(1.05)

    plt.savefig(output_location+'/plot_feature_' + feature_id + '.png')
    plt.close()


def get_polyName(feature):
    'function for QLD shapefile types'
    id_list = ['OBJECTID', 'Identifier']
    name_list = ['CATCHMENT', 'WetlandNam', 'Name']
    extra_name_list = ['HAB', 'Subwetland', 'SystemType']
    for it in id_list:
        ID = feature['properties'].get(it, '')
        if ID != '':
            break
    for it in name_list:
        CATCHMENT = feature['properties'].get(it, '')
        if CATCHMENT != '':
            break
    for it in extra_name_list:
        HAB = feature['properties'].get(it, '')
        if HAB != '':
            break
    polyName = f'{ID}_{CATCHMENT}_{HAB}'
    return(polyName)

def per_polygon_non_dask(dc, fc_product, query, datasets, shape, crs, count_pixels):
    contain, intersect = intersect_with_landsat(shape)
    print("contain by", contain)
    print("intersect with", intersect)
    query_poly = Geometry(shape['geometry'], CRS(crs))
    query['geopolygon'] = query_poly

    if datasets is None:
        datasets = fc_product.query(dc, **query)

    datasets.geopolygon = query_poly
    grouped = fc_product.group(datasets, **query)
    grouped.box = grouped.box.sortby('time')

    fc_results = None
    water_results = None

    def split_water(i_start, i_end):
        results = None
        to_split = VirtualDatasetBox(grouped.box.sel(time=grouped.box.time.data[i_start:i_end]), grouped.geobox,
                grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
        print("fetch", to_split)
        results = fc_product.fetch(to_split, **query)
        return results.drop('water'), results.water

    i = 0
    perc = None
    while i < grouped.box.time.size:
        j = 1
        if contain == {} and np.array(intersect.values()).any():
            while i+j < grouped.box.time.size:
                if np.abs(grouped.box.time.data[i] - grouped.box.time.data[i+j]).astype('timedelta64[D]') >= np.timedelta64(15, 'D'):
                    break
                j+= 1

        fc_results, water_results = split_water(i, i+j)

        if contain == {} and np.array(intersect.values()).any() and i < grouped.box.time.size-1 :
            aggregate_fc, aggregate_water = aggregate_data(fc_results, water_results)
            aggregate_fc.attrs = fc_results.attrs
            for var in fc_results.data_vars:
                aggregate_fc[var].attrs = fc_results[var].attrs
            aggregate_water.attrs = water_results.attrs
        else:
            aggregate_fc =  fc_results
            aggregate_water =  water_results
        
        print("aggregate time", aggregate_fc.time)
        if i == 0:
            mask_array = generate_raster([shape['geometry']], water_results, crs, False)
            total_pixels = np.count_nonzero(mask_array)

        i += j

        if count_pixels:
            count, valid_area = calculate_dom_area(aggregate_fc, aggregate_water, mask_array)
        else:
            count, valid_area = calculate_area(aggregate_fc, aggregate_water, mask_array, true_valid=False)

        if valid_area is None:
            valid_area = count.to_array().sum(dim='variable')
        else:
            valid_area += count['water']

        print("valid area perc", valid_area/total_pixels)
        if valid_area/total_pixels > 0.9:
            if perc is None:
                perc = (count/valid_area * 100.)
            else:
                perc = xr.concat([perc, (count/valid_area * 100.)], dim='time')
        print("area percent", perc)

    return perc


def per_polygon(dc, fc_product, query, datasets, shape, crs, count_pixels):
    contain, intersect = intersect_with_landsat(shape)
    print("contain by", contain)
    print("intersect with", intersect)
    query_poly = Geometry(shape['geometry'], CRS(crs))
    query['geopolygon'] = query_poly

    if datasets is None:
        datasets = fc_product.query(dc, **query)

    datasets.geopolygon = query_poly
    grouped = fc_product.group(datasets, **query)

    results = fc_product.fetch(grouped, **query, dask_chunks={'time':1, 'x':-1, 'y':-1})
    results = results.sortby('time')

    fc_results = results.drop('water')
    water_results = results.water

    if contain == {} and np.array(intersect.values()).any():
        print("all time", fc_results.time.size)
        fc_results, water_results = aggregate_over_time(fc_results, water_results)

    print("aggregate time", fc_results.time.size)
    mask_array = generate_raster([shape['geometry']], water_results, crs, True)
    if count_pixels:
        count, valid_area = calculate_dom_area(fc_results, water_results, mask_array)
    else:
        count, valid_area = calculate_area(fc_results, water_results, mask_array, true_valid=False)
    total_pixels = da.count_nonzero(mask_array)

    print("loading")
    count.load()

    if valid_area is None:
        valid_area = count.to_array().sum(dim='variable')
    else:
        valid_area.load()
        valid_area += count['water']

    perc = (count/valid_area * 100.).sel(time=(valid_area/total_pixels > 0.9))
    print("area percent", perc)
    return perc

@click.command(name='wetland_insignt')
@click.argument('shapefile', type=str, default='/g/data/r78/rjd547/DES-QLD_Project/data/Wet_WGS84_P.shp')
@click.option('--start-date',  type=str, help='Start date, default=1987-01-01', default='1987-01-01')
@click.option('--end-date',  type=str, help='End date, default=2020-01-01', default='2019-12-31')
@click.option('--start-feature',  type=int, help='Start from a feature, should be with the range of shape list', default=0)
@click.option('--end-feature',  type=int, help='The last feature to be done', default=None)
@click.option('--datasets',  type=str, help='Pickled datasets', default=None)
@click.option('--count-pixels',  type=bool, help='Backward compatible to counting the dominant pixels than calculating the actual area in fc', 
        default=False)
@click.option('--output-location', type=str, help='Location for output data files', default='./')
@click.option('--enable-dask', type=bool, help='With dask', default=False)
@click.option('--workers', type=int, help='Dask worker numbers', default=4)

def main(shapefile, start_date, end_date, start_feature, end_feature, datasets, count_pixels,
         output_location, enable_dask, workers):
    if enable_dask:
        dask_worker_dir = 'dask-worker-space/' + str(start_feature) 
        cluster = LocalCluster(n_workers=workers, local_directory=dask_worker_dir)
        client = Client(cluster) 
    
    if datasets == 'None':
        datasets = None

    if datasets is None:
        dc = Datacube()
    else:
        dc= None

    if not path.exists(output_location):
        os.makedirs(output_location, exist_ok=True)

    pd_yaml = '/g/data1a/u46/users/ea6141/wlinsight/wetland_pd.yaml'
    with open(pd_yaml, 'r') as f:
        recipe = yaml.safe_load(f)
        fc_product = construct(**recipe)

    poly_path = shapefile
    start_id = int(start_feature) 
    if end_feature is not None:
        end_id = int(end_feature)
    else:
        end_id = -1
    with fiona.open(poly_path) as allshapes:
        #get the crs of the polygon file to use when processing each polygon
        crs = allshapes.crs_wkt
        #get the list of all the shapes in the shapefile
        for shape in allshapes:
            pl_name = shape['id']

            if end_id >= 0 and end_id < int(pl_name):
                break
            if start_id > int(pl_name):
                continue

            polyName = get_polyName(shape)
            print("polyname", polyName)
            query = {'time': (start_date, end_date)}

            if datasets is not None:
                with open(datasets, 'rb') as f:
                    datasets = pickle.load(f)

            if enable_dask:
                perc = per_polygon(dc, fc_product, query, datasets, shape, crs, count_pixels)
            else:
                perc = per_polygon_non_dask(dc, fc_product, query, datasets, shape, crs, count_pixels)
            print("area percent in year", perc)
            id_list = ['ORIG_FID', 'SYSID']
            for it in id_list: 
                o_id = str(shape['properties'].get(it, 0))
            plot_to_png(perc, '_'.join([pl_name, o_id]), polyName, output_location)
            perc.to_dataframe().to_csv(output_location + '/area_percent_' + pl_name + '_' + o_id +  '.csv')

    if enable_dask:
        client.close()
    return

if __name__ == '__main__':
    main()
