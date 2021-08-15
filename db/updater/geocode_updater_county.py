from geocode.db.updater.updater import Updater


class GeocodeUpdaterCounty(Updater):
    def __init__(self, conn, county_table):
        super().__init__(conn)
        self.__geographic_layer_county_table = county_table

    def set_geographic_layer_county_table(self, a):
        self.__geographic_layer_county_table = a

    def __get_geographic_layer_county_table(self):
        return self.__geographic_layer_county_table

    def update_data(self) -> dict:
        query = f"""
        UPDATE county
        SET geometry = gs.wkb_geometry
        FROM {self.__get_geographic_layer_county_table()} as gs
        WHERE fn_clearstring(gs.shahrestan) = fn_clearstring(county.name);"""
        resp = self._updater(query)
        return resp["msg"]
