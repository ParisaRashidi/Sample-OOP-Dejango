from geocode.db.clients.db_client import DBClient
from geocode.db.updater.geocode_updater_building import GeocodeUpdaterBuilding
from geocode.db.updater.geocode_updater_county import GeocodeUpdaterCounty
from geocode.db.updater.geocode_updater_parish import GeocodeUpdaterParish
from geocode.db.updater.geocode_updater_part import GeocodeUpdaterPart
from geocode.db.updater.geocode_updater_province import GeocodeUpdaterProvince
from geocode.db.updater.geocode_updater_tour import GeocodeUpdaterTour
from geocode.db.updater.geocode_updater_zone import GeocodeUpdaterZone
from django.db import connections
from gnaf_dataflow.settings import config


class PostgresClient(DBClient):
    def __init__(self):
        super().__init__()
        self.__conn = {
            'ENGINE': config.get('DB', 'ENGINE'),
            'HOST': config.get('DB', 'HOST'),
            'PORT': config.get('DB', 'PORT'),
            'NAME': config.get('DB', 'NAME'),
            'USER': config.get('DB', 'USER'),
            'PASSWORD': config.get('DB', 'PASSWORD')
        }

    def update_geometry_data_building(self, city_id: str, geo_table: str, parcell_table: str) -> dict:
        print("start client")
        # create update objects
        gcub = GeocodeUpdaterBuilding(
            conn=connections[config.get('DB', 'ALIAS')],
            city_id=city_id,
            geo_table=geo_table,
            parcell_table=parcell_table,
        )
        # call update modules
        building_updater_dic: dict = gcub.update_data()
        return {"updater_geocode_building": building_updater_dic}

    def update_geometry_data_county(self, county_table: str) -> dict:
        # create update objects
        gcuc = GeocodeUpdaterCounty(
            conn=connections[config.get('DB', 'ALIAS')],
            county_table=county_table,
        )
        # call update modules
        county_updater_dic: dict = gcuc.update_data()
        return {"updater_geocode_county": county_updater_dic}

    def update_geometry_data_parish(self, parish_table: str) -> dict:
        # create update objects
        gcup = GeocodeUpdaterParish(
            conn=connections[config.get('DB', 'ALIAS')],
            parish_table=parish_table,
        )
        # call update modules
        parish_updater_dic: dict = gcup.update_data()
        return {"updater_geocode_parish": parish_updater_dic}

    def update_geometry_data_part(self, part_table: str, tour_table: str) -> dict:
        # create update objects
        gcupart = GeocodeUpdaterPart(
            conn=connections[config.get('DB', 'ALIAS')],
            part_table=part_table,
            tour_table=tour_table,
        )
        # call update modules
        part_updater_dic: dict = gcupart.update_data()
        return {"updater_geocode_part": part_updater_dic}

    def update_geometry_data_province(self, province_table: str) -> dict:
        # create update objects
        gcupro = GeocodeUpdaterProvince(
            conn=connections[config.get('DB', 'ALIAS')],
            province_table=province_table,
        )
        # call update modules
        province_updater_dic: dict = gcupro.update_data()
        return province_updater_dic

    def update_geometry_data_tour(self, tour_table: str) -> dict:
        # create update objects
        gcut = GeocodeUpdaterTour(
            conn=connections[config.get('DB', 'ALIAS')],
            tour_table=tour_table,
        )
        # call update modules
        tour_updater_dic: dict = gcut.update_data()
        return {"updater_geocode_tour": tour_updater_dic}

    def update_geometry_data_zone(self, zone_table: str) -> dict:
        # create update objects
        gcuz = GeocodeUpdaterZone(
            conn=connections[config.get('DB', 'ALIAS')],
            zone_table=zone_table,
        )
        # call update modules
        zone_updater_dic: dict = gcuz.update_data()
        return {"updater_geocode_zone": zone_updater_dic}
