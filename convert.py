import geojson
import os

from fabric.api import local, puts
from shapely.geometry import shape

# Properties included in cartographic boundary file
# Comment out ones we want to keep in the geojson

AREA_CONFIG = {
    'zipcode': {
        'id': 'ZCTA5CE10',
        'properties': [
            # 2010 Census 5-digit ZIP Code Tabulation Area code
            'ZCTA5CE10',    # '95832'
            # 2010 Census 5-digit ZIP Code Tabulation Area identifier
            'GEOID10',      # '95832'
            # Different version of GEO ID above
            'AFFGEOID10',   # '8600000US36426'
            # 2010 Census land area (square meters)
            # 'ALAND10',      # 21414053.0
            # 2010 Census water area (square meters)
            # 'AWATER10',     # 1511680.0
        ],
    },
    'state': {
        'id': 'STUSPS',
        'properties': [
            'AFFGEOID',    # '0400000US02',
            # 'ALAND',       # 1477849359548.0,
            # 'AWATER',      # 245487700921.0,
            'GEOID',       # '02',
            'LSAD',        # '00',
            # 'NAME',        # 'Alaska',
            'STATEFP',     # '02',
            'STATENS',     # '01785533',
            # 'STUSPS',      # 'AK'
        ],
    },
    'region': {
        'id': 'NAME',
        'properties': [
            'AFFGEOID',     # '0200000US1',
            # 'ALAND',        # 419356559348.0,
            # 'AWATER',       # 50260795559.0,
            # 'GEOID',        # '1',
            'LSAD',         # '68',
            # 'NAME',         # 'Northeast',
            # 'REGIONCE',     # '1'
        ],
    },
    'nation': {
        'id': 'GEOID',
        'properties': [
            'AFFGEOID',     # '0100000US'
            # 'GEOID',        # 'US'
            # 'NAME',         # 'United States'
        ],
    },
}


def convert_shape(*args, **kwargs):
    area = Area(*args, **kwargs)
    area.update_shape()
    area.write_to_file()


class Area(object):
    def __init__(self, area_type, shape, topojson=False):
        if area_type not in AREA_CONFIG.keys():
            raise StandardError('Invalid area type')

        self.area_type = area_type
        self.shape = shape
        self.topojson = topojson

    def update_shape(self):
        config = AREA_CONFIG[self.area_type]

        self.shape['id'] = self.shape['properties'][config['id']]
        for prop in config['properties']:
            del self.shape['properties'][prop]

        polygon = shape(self.shape['geometry'])
        self.shape['geometry'] = polygon.simplify(
            0.0003,
            preserve_topology=True
        )

    def write_to_file(self):
        filename = self._get_filename()

        geojson_filename = 'geojson/{0}/{1}.geojson'.format(
            self.area_type,
            filename,
        )

        if not os.path.exists(os.path.dirname(geojson_filename)):
            os.makedirs(os.path.dirname(geojson_filename))

        with open(geojson_filename, 'w') as sink:
            geojson.dump(self.shape, sink, indent=None)

        if self.topojson:
            topojson_filename = 'topojson/{0}/{1}.topojson'.format(
                self.area_type,
                filename,
            )

            if not os.path.exists(os.path.dirname(topojson_filename)):
                os.makedirs(os.path.dirname(topojson_filename))

            local('topojson -o {0} {1} -p'.format(
                geojson_filename,
                topojson_filename,
            ))

        puts('{0}\t...{1}\t...done'.format(filename, os.getpid()))

    def _get_filename(self):
        filename_func = getattr(
            self,
            '_{0}_filename'.format(self.area_type),
            None,
        )
        if filename_func:
            return filename_func()
        else:
            return self.shape['id']

    def _zipcode_filename(self):
        return '{0}/{1}'.format(
            self.shape['id'][:2],
            self.shape['id'],
        )
