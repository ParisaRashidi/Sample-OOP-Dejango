from geocode.db.updater.updater import Updater


class GeocodeUpdaterPart(Updater):
    def __init__(self, conn, tour_table: str, part_table: str):
        super().__init__(conn)
        self.__geographic_layer_part_table = part_table
        self.__geographic_layer_tour_table = tour_table

    def set_geographic_layer_part_table(self, a):
        self.__geographic_layer_part_table = a

    def __get_geographic_layer_part_table(self):
        return self.__geographic_layer_part_table

    def set_geographic_layer_tour_table(self, a):
        self.__geographic_layer_tour_table = a

    def __get_geographic_layer_tour_table(self):
        return self.__geographic_layer_tour_table

    def update_data(self) -> dict:
        query: str = f"""
                UPDATE part
                SET geometry = gj.geom
                FROM 
                (SELECT gasht.gasht_name,
                        gasht.id as gasht_id,
                        joze.joze,
                        joze.geom   
                FROM {self.__get_geographic_layer_part_table()} as joze
                JOIN {self.__get_geographic_layer_tour_table()} as gasht
                ON gasht.gasht_name::text = joze.gasht)  AS gj 
                WHERE gj.joze = part.name AND part.tour_id = gj.gasht_id;
                """
        resp = self._updater(query)
        return resp
