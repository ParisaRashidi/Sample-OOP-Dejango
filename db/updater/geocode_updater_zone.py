from geocode.db.updater.updater import Updater


class GeocodeUpdaterZone(Updater):
    def __init__(self, conn, zone_table):
        super().__init__(conn)
        self.__geographic_layer_zone_table = zone_table

    def set_geographic_layer_zone_table(self, a):
        self.__geographic_layer_zone_table = a

    def __get_geographic_layer_zone_table(self):
        return self.__geographic_layer_zone_table

    def update_data(self) -> dict:
        query = f"""
        UPDATE zone
        SET geometry = gb.wkb_geometry
        FROM {self.__get_geographic_layer_zone_table()} AS gb
        WHERE fn_clearstring(gb.bakhsh) = fn_clearstring(zone.name);
        """
        resp = self._updater(query)
        return resp["msg"]
