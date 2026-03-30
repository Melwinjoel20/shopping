from django.contrib import admin
from django.urls import path
from . import views
from . import admins_view as views_admin

urlpatterns = [
     path('base/',views.base,name="base"),
     path('home/',views.home,name="home"),
     path("register/", views.register, name="register"),
     path("login/", views.login_view, name="login"),
     path("logout/", views.logout_view, name="logout"),
     path("products/", views.products, name="products"),
     path("products/<str:category>/", views.products, name="products"),
     path("cart/", views.view_cart, name="view_cart"),
     path("checkout/", views.checkout, name="checkout"),
     path("order-confirmation/", views.order_confirmation, name="order_confirmation"),
     path("verify-otp/", views.verify_otp, name="verify_otp"),
     path("forgot_password/", views.forgot_password, name="forgot_password"),
     path("reset_password/", views.reset_password, name="reset_password"),
     path("admin_dashboard/", views_admin.admin_dashboard, name="admin_dashboard"),
     path("add-product/", views_admin.admin_add_product, name="admin_add_product"),
     path("manage-products/", views_admin.admin_manage_products, name="admin_manage_products"),
     path("delete/<str:category>/<str:product_id>/", views_admin.admin_delete_product, name="admin_delete_product"),
     path("admin-sales-dashboard/", views_admin.admin_sales_dashboard, name="admin_sales_dashboard"),
     path('get-location/', views.get_location, name='get_location'),
     path("offers/", views.offers, name="offers"),
     path("trigger-sales-report/", views_admin.trigger_sales_report, name="trigger_sales_report"),
     path("check-report-status/", views_admin.check_report_status, name="check_report_status"),


]
