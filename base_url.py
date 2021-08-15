from django.urls import path, include

urlpatterns = [
    path('', include('geocode.urls.updater_urls')),

]
