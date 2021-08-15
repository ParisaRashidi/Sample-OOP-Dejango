from geocode.db.updater.updater import Updater


class GeocodeUpdaterParish(Updater):
    def __init__(self, conn, parish_table):
        super().__init__(conn)
        self.__geographic_layer_parish_table = parish_table

    def set_geographic_layer_parish_table(self, a):
        self.__geographic_layer_parish_table = a

    def __get_geographic_layer_parish_table(self):
        return self.__geographic_layer_parish_table

    def update_data(self) -> dict:
        query = f"""
        UPDATE parish
        SET geometry = gm.wkb_geometry
        FROM {self.__get_geographic_layer_parish_table()} gm
        WHERE fn_clearstring(gm.namemahale) = fn_clearstring(parish.name);"""
        resp = self._updater(query)
        return resp
