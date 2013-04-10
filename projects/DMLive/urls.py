from django.conf.urls.defaults import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    (r'mainform', 'monitoring.views.mainform'),
    (r'search_results_by_client_([a-zA-Z0-9]+)', 'monitoring.views.search_results_by_client'),
    (r'search_results_by_strategy_([*a-zA-Z0-9]+)_([a-zA-Z0-9]+)', 'monitoring.views.search_results_by_strategy'),
    (r'search_results_by_order_([a-zA-Z0-9]+)', 'monitoring.views.search_results_by_order'),
    (r'real_time_view_([^_]+)_([0-9]+)', 'monitoring.views.real_time_view'),
    (r'img/plot_view_([a-zA-Z0-9]+)_([0-9]+)', 'monitoring.views.plot_view'),
    (r'img/pie_exec_quantity_([a-zA-Z0-9]+)', 'monitoring.views.client_overview')
    # Examples:
    # url(r'^$', 'DMLive.views.home', name='home'),
    # url(r'^DMLive/', include('DMLive.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)

