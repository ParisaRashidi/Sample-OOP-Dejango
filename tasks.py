from celery import shared_task

from geocode.db.clients.postgres_client import PostgresClient
from gnaf_dataflow.celery import app
from gnaf_dataflow.settings import config


@app.task
def update_building_set_geometry(city_id, geo_table) -> dict:
    print("inside task")
    return city_id + geo_table
    # try:
    #     print("inside task")
    #     postgres_client = PostgresClient(
    #         conn={
    #             'ENGINE': config.get('DB', 'ENGINE'),
    #             'HOST': config.get('DB', 'HOST'),
    #             'PORT': config.get('DB', 'PORT'),
    #             'NAME': config.get('DB', 'NAME'),
    #             'USER': config.get('DB', 'USER'),
    #             'PASSWORD': config.get('DB', 'PASSWORD')
    #         },
    #         city_id=city_id,
    #         geo_table=geo_table,
    #         parcell_table=parcell_table
    #     )
    #     resp: dict = postgres_client.update_geometry_data_building()
    #     return resp
    # except Exception as err:
    #     print(err)

#
# @app.task
# def update_county_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_county()
#         return resp
#     except Exception as err:
#         print(err)
#
#
# @app.task
# def update_parish_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_parish()
#         return resp
#     except Exception as err:
#         print(err)
#
#
# @app.task
# def update_part_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_part()
#         return resp
#     except Exception as err:
#         print(err)
#
#
# @app.task
# def update_province_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_province()
#         return resp
#     except Exception as err:
#         print(err)
#
#
# @shared_task
# def update_tour_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_tour()
#         return resp
#     except Exception as err:
#         print(err)
#
#
# @app.task
# def update_zone_set_geometry() -> dict:
#     try:
#         postgres_client = PostgresClient()
#         resp: dict = postgres_client.update_geometry_data_zone()
#         return resp
#     except Exception as err:
#         print(err)
