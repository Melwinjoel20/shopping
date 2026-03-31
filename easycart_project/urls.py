from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView

urlpatterns = [
    path('store/admin/', admin.site.urls),
    path('store/', include('store.urls')),
    path('', RedirectView.as_view(url='/store/home/')),  # add this line
]