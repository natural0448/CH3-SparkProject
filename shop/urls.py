from django.urls import path

from . import views


app_name = "shop"
urlpatterns = [
    path("products/", views.products, name="products"),
    path("orders/", views.orders, name="orders"),
    path("formview", views.formview),
    # formprocess 뷰 함수를 구현한 뒤 아래 경로를 활성화하세요.
    path("formprocess", views.formprocess),
]
