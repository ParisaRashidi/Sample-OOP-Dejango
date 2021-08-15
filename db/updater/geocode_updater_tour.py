from geocode.db.updater.updater import Updater


class GeocodeUpdaterTour(Updater):
    def __init__(self, conn, tour_table):
        super().__init__(conn)
        self.__geographic_layer_tour_table = tour_table

    def set_geographic_layer_tour_table(self, a):
        self.__geographic_layer_tour_table = a

    def __get_geographic_layer_tour_table(self):
        return self.__geographic_layer_tour_table

    def update_data(self) -> dict:
        query = f"""
        UPDATE tour
        SET geometry = gg.wkb_geometry
        FROM {self.__get_geographic_layer_tour_table()} gg
        WHERE gg.gasht_name::text = tour.name;
        """
        resp = self._updater(query)
        return resp
