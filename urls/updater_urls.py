from django.urls import path
import geocode.views.geocode_updater_view as geocode_view

urlpatterns = [

    path('building', geocode_view.geocode_updater_view_building),
    path('province', geocode_view.geocode_updater_view_province),
    path('county', geocode_view.geocode_updater_view_county),
    path('zone', geocode_view.geocode_updater_view_zone),
    path('tour', geocode_view.geocode_updater_view_tour),
    path('parish', geocode_view.geocode_updater_view_parish),
    path('part', geocode_view.geocode_updater_view_part),
]
