from abc import ABC, abstractmethod


class Updater(ABC):
    def __init__(self, conn):
        self._conn = conn

    def _updater(self, query: str) -> dict:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(query)
                return {'success': True, 'msg': cursor.statusmessage}
        except Exception as err:
            print(err)
            return {'success': False, 'msg': cursor.statusmessage}

    @abstractmethod
    def update_data(self) -> dict:
        pass
