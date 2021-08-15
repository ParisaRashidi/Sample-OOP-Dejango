import abc
from datetime import datetime
from typing import Optional

from django.db import connections
from hashids import Hashids
from gnaf_dataflow.settings import config


class DBClient(abc.ABC):
    def __init__(self):
        self._con_props = {
                'ENGINE': config.get('DB', 'ENGINE'),
                'HOST': config.get('DB', 'HOST'),
                'PORT': config.get('DB', 'PORT'),
                'NAME': config.get('DB', 'NAME'),
                'USER': config.get('DB', 'USER'),
                'PASSWORD': config.get('DB', 'PASSWORD')
            }
        self._db_name = self._con_props['NAME']
        self._con_id = self._add_connection()

    def _add_connection(self) -> Optional[str]:
        try:
            hashes = Hashids(min_length=15)
            con_id = hashes.encode(datetime.now().timestamp())
            connections.databases[con_id] = self._con_props
            return con_id
        except Exception as err:
            print(err)
            return None
