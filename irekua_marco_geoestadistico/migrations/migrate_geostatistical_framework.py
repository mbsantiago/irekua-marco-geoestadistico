import os
import datetime
import zipfile
import json
import logging

from tqdm import tqdm

from django.contrib.gis.gdal import DataSource
from django.contrib.gis.gdal import CoordTransform
from django.contrib.gis.gdal import OGRGeometry
from django.contrib.gis.gdal import OGRGeomType

from django.db import migrations
from django.db import connections
from django.db import router


logging.basicConfig(level=logging.INFO)

BASEDIR = os.path.dirname(os.path.abspath(__file__))
TARGET_DIR = os.path.join(BASEDIR, 'extracted_data')
INEGI_DESCRIPTION = '''El Marco Geoestadístico (MG) Integrado se conforma por información vectorial, tablas de atributos y catálogos.
Muestra la división geoestadística del territorio nacional en sucesivos niveles de desagregación. Esta división está dada por los llamados LÍMITES GEOESTADÍSTICOS, que pueden coincidir con los límites político-administrativos oficiales, los cuales tienen sustento legal; sin embargo, los que no cuentan con dicho sustento deben entenderse como límites provisionales, trazados sólo para realizar los operativos censales. Estos límites provisionales no tienen pretensión de oficialidad, dado que el Instituto Nacional de Estadística y Geografía no es el órgano facultado para definir límites político-administrativos.
El MG contiene además la cobertura de todas las localidades del territorio nacional, de manera que a cada una de las viviendas le corresponde una secuencia de claves de identificación geográfica que está dada por los sucesivos niveles de desagregación en los que se divide el territorio nacional.
'''
BASIC_SCHEMA= {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "INEGI Marco Geoestadístico 2018",
    "required": [],
    "properties": {}
}


def migrate_geostatistical_framework(apps, schema_editor):
    if not data_is_unpacked():
        unpack_data()

    entity_migrator = EntityMigrator(apps)
    entity_migrator.migrate()

    stores = {'entities': entity_migrator.localities}
    municipality_migrator = MunicipalityMigrator(apps, stores=stores)
    municipality_migrator.migrate()

    stores['municipalities'] = municipality_migrator.localities
    locality_migrator = LocalityMigrator(apps, stores=stores)
    locality_migrator.migrate()


def data_is_unpacked():
    return os.path.exists(TARGET_DIR)


def unpack_data():
    logging.info('Extracting zip file with geostatistical framework')

    for basename in ['l.zip', 'mun.zip', 'ent.zip']:
        zip_file = os.path.join(BASEDIR, 'data', basename)

        with zipfile.ZipFile(zip_file, 'r') as zfile:
            zfile.extractall(TARGET_DIR)

    logging.info('Extraction done')


class Migrator(object):
    name = 'Migrator'
    file_name = ''
    attributes = []

    def __init__(self, apps, stores=None):
        self.logger = logging.getLogger(self.name)

        self.locality_model = apps.get_model('irekua_database.Locality')
        self.locality_type_model = apps.get_model('irekua_database.LocalityType')

        self.spatial_backend = connections[router.db_for_write(self.locality_model)].ops

        self.localities = {}
        self.stores = stores

    def migrate(self):
        self.logger.info('Migrating %s', self.name)

        shape_file = os.path.join(TARGET_DIR, self.file_name)
        source = DataSource(shape_file)
        layer = source[0]

        self.locality_type = self.create_type(layer)
        self.transform = self.get_transform(layer)

        for feature in tqdm(layer):
            self.create_locality_from_feature(feature)

        self.logger.info('Done migrating %s', self.name)

    def create_locality_from_feature(self, feature):
        name = feature.get('NOMGEO')

        if feature.geom_type == 'Polygon':
            geometry = OGRGeometry(OGRGeomType('MultiPolygon'))
            geometry.add(feature.geom)
        else:
            geometry = feature.geom
        geometry.transform(self.transform)

        metadata = self.get_feature_metadata(feature)
        locality = self.locality_model.objects.create(
            name=name,
            geometry=geometry.wkt,
            locality_type=self.locality_type,
            metadata=metadata)

        self.create_locality_implications(feature, locality)

    def create_type(self, layer):
        metadata_schema = self.create_metadata_schema()
        name = 'MARCO GEOESTADÍSTICO INTEGRADO, DICIEMBRE  2018 (%s)' % self.name
        publication_date = datetime.date(year=2018, month=12, day=1)
        source = 'https://www.inegi.org.mx/temas/mg/default.html'
        original_datum = layer.srs.wkt

        return self.locality_type_model.objects.create(
            metadata_schema=metadata_schema,
            name=name,
            publication_date=publication_date,
            source=source,
            description=INEGI_DESCRIPTION,
            original_datum=original_datum)

    def create_locality_implications(self, feature, locality):
        pass

    def create_metadata_schema(self):
        schema = BASIC_SCHEMA.copy()
        schema['title'] = self.name + ' ' + schema['title']

        for name, title in self.attributes:
            schema['required'].append(name)
            schema['properties'][name] = {
                "type": "integer",
                "title": title
            }

        return json.dumps(schema)

    def get_feature_metadata(self, feature):
        return {
            field: feature.get(field)
            for field, _ in self.attributes
        }

    def get_transform(self, layer):
        source_srs = layer.srs
        target_srid = self.locality_model._meta.get_field('geometry').srid
        SpatialRefSys = self.spatial_backend.spatial_ref_sys()
        target_srs = SpatialRefSys.objects.get(srid=target_srid).srs
        return CoordTransform(source_srs, target_srs)


class EntityMigrator(Migrator):
    name = 'Entidad'
    file_name = '01_32_ent.shp'
    attributes = [
        ('CVEGEO', 'Clave de geometria'),
        ('CVE_ENT', 'Clave de entidad')
    ]

    def create_locality_implications(self, feature, locality):
        self.localities[feature.get('CVE_ENT')] = locality


class MunicipalityMigrator(Migrator):
    name = 'Municipio'
    file_name = '01_32_mun.shp'
    attributes = [
        ('CVEGEO', 'Clave de geometria'),
        ('CVE_ENT', 'Clave de entidad'),
        ('CVE_MUN', 'Clave de municipio'),
    ]

    def create_locality_implications(self, feature, locality):
        entity = self.stores['entities'][feature.get('CVE_ENT')]
        locality.is_part_of.add(entity)
        self.localities[feature.get('CVE_MUN')] = locality


class LocalityMigrator(Migrator):
    name = 'Localidad'
    file_name = '01_32_l.shp'
    attributes = [
        ('CVEGEO', 'Clave de geometria'),
        ('CVE_ENT', 'Clave de entidad'),
        ('CVE_MUN', 'Clave de municipio'),
        ('CVE_LOC', 'Clave de localidad'),
    ]

    def create_locality_implications(self, feature, locality):
        entity = self.stores['entities'][feature.get('CVE_ENT')]
        municipality = self.stores['municipalities'][feature.get('CVE_MUN')]
        locality.is_part_of.add(entity, municipality)


class Migration(migrations.Migration):
    dependencies = [
        ('database', '0008_locality_localitytype')
    ]

    operations = [
        migrations.RunPython(migrate_geostatistical_framework)
    ]
