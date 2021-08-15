from django.template import RequestContext
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
import rest_framework.status as status
from rest_framework import viewsets
from geocode.db.clients.postgres_client import PostgresClient
from gnaf_dataflow.settings import config


@api_view(['PUT'])
def update_geo_layer(request: Request, geo_layer_type: str):
    try:
        if geo_layer_type in ('province','county','district','tour','parish','part'):
            get_connection_to_postgres_client(geo_layer_type)
            return Response(data={'message' : 'update geocode'}, status= status.HTTP_200_OK)
        else:
            return Response(data={'message': 'geo_layer must be one of  province,county,district,tour,parish,part'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as err:
        print(err)
        return Response(data={'message': err.__str__()}, status=status.HTTP_400_BAD_REQUEST)


def get_connection_to_postgres_client(geo_layer_type: str) -> dict:
    postgres_client = PostgresClient(
        con_props={
            'ENGINE': config.get('DB', 'ENGINE'),
            'HOST': config.get('DB', 'HOST'),
            'PORT': config.get('DB', 'PORT'),
            'NAME': config.get('DB', 'NAME'),
            'USER': config.get('DB', 'USER'),
            'PASSWORD': config.get('DB', 'PASSWORD')
        },

    )
    resp: dict = postgres_client.update_geo_data(geo_layer_type)
    return resp
