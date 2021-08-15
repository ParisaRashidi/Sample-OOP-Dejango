from geocode.db.updater.updater import Updater


class GeocodeUpdaterBuilding(Updater):

    def __init__(self, conn, city_id: str, geo_table: str, parcell_table: str):
        super().__init__(conn)
        self.__city_id = city_id
        self.__geographic_layer_geo_table = geo_table
        self.__geographic_layer_parcell_table = parcell_table

    def set_city_id(self, a):
        self.__city_id = a

    def __get_city_id(self):
        return self.__city_id

    def set_geographic_layer_geo_table(self, a):
        self.__geographic_layer_geo_table = a

    def __get_geographic_layer_geo_table(self):
        return self.__geographic_layer_geo_table

    def set_geographic_layer_parcell_table(self, a):
        self.__geographic_layer_parcell_table = a

    def __get_geographic_layer_parcell_table(self):
        return self.__geographic_layer_parcell_table

    def update_data(self):
        print("start query")
        query = f"""
        UPDATE building AS b
        SET geometry = t.wkb_geometry
        FROM (SELECT g.gnafcode,p.wkb_geometry FROM {self.__get_geographic_layer_geo_table()} g
        JOIN {self.__get_geographic_layer_parcell_table()} p
        ON st_contains(p.wkb_geometry,g.wkb_geometry))t
        WHERE population_point_id = {self.__get_city_id()}::integer AND substring(gnafcode::text from '([0-9]+)(.*)') = b.gnaf_code;"""
        resp = self._updater(query)
        return resp["msg"]
