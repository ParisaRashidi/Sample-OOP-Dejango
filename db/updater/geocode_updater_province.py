from geocode.db.updater.updater import Updater


class GeocodeUpdaterProvince(Updater):
    def __init__(self, conn, province_table):
        super().__init__(conn)
        self.__geographic_layer_province_table = province_table

    def set_geographic_layer_province_table(self, a):
        self.__geographic_layer_province_table = a

    def __get_geographic_layer_province_table(self):
        return self.__geographic_layer_province_table

    def update_data(self) -> dict:
        query = f"""
         UPDATE province
         SET geometry = go.wkb_geometry
         FROM {self.__get_geographic_layer_province_table()} AS go
         WHERE fn_clearstring(go.ostan) = fn_clearstring(province.name);
         """
        resp = self._updater(query)
        return resp["msg"]
