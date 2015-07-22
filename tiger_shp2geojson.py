"""
Create geoJSON for US zipcodes based on Census Bureau ZCTA shapes.

## Installation

# Install gcc, python dev extensions, GDAL
# On Ubuntu:
sudo apt-get install gcc python-dev libgdal1-dev

# Install Python Dependencies
pip install -r requirements.txt

# Download ZIP Code Tabulation Area (ZCTA) Shape files from
#   the US Census Bureau.
mkdir data
cd data
wget http://www2.census.gov/geo/tiger/GENZ2014/shp/cb_2014_us_zcta510_500k.zip
unzip -d zipcode cb_2014_us_zcta510_500k.zip


"""
import convert
import fiona
import os
import sys

from fabric.api import local, puts
from multiprocessing import Pool


FILE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_DIRECTORY_PATH = FILE_DIRECTORY_PATH + '/data'

# Cartographic Boundary data
AREA_TYPE_FILENAMES = {
    'zipcode': 'cb_2014_us_zcta510_500k.shp',
    'state': 'cb_2014_us_state_500k.shp',
    'nation': 'cb_2014_us_nation_5m.shp',
    'region': 'cb_2014_us_region_500k.shp',
}


def main():
    area_type = None
    valid_area_types = ['zipcode', 'state', 'region', 'nation']

    try:
        area_type = sys.argv[1]
    except IndexError:
        puts('Area type not found.')

    if area_type and area_type not in valid_area_types:
        puts('Invalid area type {0}.'.format(area_type))
        area_type = None

    if not area_type:
        puts('Valid values are {0}'.format(', '.join(valid_area_types)))
        sys.exit(1)

    output_path = '{0}/{1}'.format(
        FILE_DIRECTORY_PATH,
        area_type,
    )
    shp_file_path = '{0}/{1}/{2}'.format(
        DATA_DIRECTORY_PATH,
        area_type,
        AREA_TYPE_FILENAMES[area_type],
    )

    topojson = False
    if '--topo' in sys.argv:
        topojson = True

    multi = False
    if '-m' in sys.argv:
        multi = True
        # start 4 worker processes
        pool = Pool(processes=4)

    local('mkdir -p geojson')
    if topojson:
        local('mkdir -p topojson')

    with fiona.open(shp_file_path, 'r') as source:
        count = 0
        results = []
        while (True):
            count += 1
            area = next(source, None)
            if area:
                if multi:
                    # With multiprocessing:
                    result = pool.apply_async(
                        convert.convert_shape,
                        [area_type, area, topojson],
                    )
                    results.append(result)
                else:
                    # Without multiprocessing:
                    convert.convert_shape(
                        area_type, area, topojson
                    )
            else:
                if multi:
                    [r.get() for r in results]
                print count
                break

if __name__ == '__main__':
    main()
