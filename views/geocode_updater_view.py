from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
import rest_framework.status as status
from geocode import tasks
from geocode.db.clients.postgres_client import PostgresClient
from gnaf_dataflow.settings import config


@api_view(['PUT'])
def geocode_updater_view_building(request: Request):
    if 'city_id' in request.POST and 'geo_table' in request.POST and 'parcell_table' in request.POST:
        population_point_id = request.data['population_point_id']
        geo_table = request.data['geo_table']
        parcell_table = request.data['parcell_table']
        if population_point_id != '' and geo_table != '' and parcell_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_building(
                    city_id=population_point_id,
                    geo_table=geo_table,
                    parcell_table=parcell_table
                )
                response: dict = {
                    'data': {'message': 'building geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "building" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'population_point_id and geo_table and parcell_table must not get '
                           'null values'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'population_point_id or geo_table or geographic_layer_parcell_table must exist in request '
                       'body '
                       'values'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_province(request: Request):
    if 'geographic_layer_province_table' in request.POST:
        province_table = request.data['geographic_layer_province_table']
        if province_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_province(
                    province_table=province_table
                )
                response: dict = {
                    'data': {'message': 'province geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "province" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_province_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_province_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_county(request: Request):
    if 'geographic_layer_county_table' in request.POST:
        county_table = request.data['geographic_layer_county_table']
        if county_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_county(
                    county_table=county_table
                )
                response: dict = {
                    'data': {'message': 'county geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "county" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_county_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_county_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_zone(request: Request):
    if 'geographic_layer_zone_table' in request.POST:
        zone_table = request.data['geographic_layer_zone_table']
        if zone_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_zone(
                    zone_table=zone_table
                )
                response: dict = {
                    'data': {'message': 'zone geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "zone" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_zone_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_county_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_tour(request: Request):
    if 'geographic_layer_tour_table' in request.POST:
        tour_table = request.data['geographic_layer_tour_table']
        if tour_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_tour(
                    tour_table=tour_table
                )
                response: dict = {
                    'data': {'message': 'tour geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "tour" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_tour_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_tour_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_parish(request: Request):
    if 'geographic_layer_parish_table' in request.POST:
        parish_table = request.data['geographic_layer_parish_table']
        if parish_table != '':
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_parish(
                    parish_table=parish_table
                )
                response: dict = {
                    'data': {'message': 'parish geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "parish" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_parish_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_parish_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def geocode_updater_view_part(request: Request):
    if 'geographic_layer_tour_table' in request.POST and 'geographic_layer_part_table' in request.POST:
        tour_table = request.data['geographic_layer_tour_table']
        part_table = request.data['geographic_layer_part_table']
        if tour_table != '' and part_table:
            try:
                postgres_client = PostgresClient()
                postgres_client.update_geometry_data_part(
                    tour_table=tour_table,
                    part_table=part_table
                )
                response: dict = {
                    'data': {'message': 'part geocode done'},
                    'status': status.HTTP_200_OK
                }
                return Response(data=response["data"], status=response["status"])
            except Exception as err:
                print(err)
                response: dict = {
                    'data': {'message': 'error occurred in "part" geocode'},
                    'status': status.HTTP_201_CREATED
                }
                return Response(data=response["data"], status=response["status"])
        else:
            return Response(data={
                'message': 'geographic_layer_part_table must not get null value'},
                status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(data={
            'message': 'geographic_layer_part_table must exist in request'},
            status=status.HTTP_400_BAD_REQUEST)
