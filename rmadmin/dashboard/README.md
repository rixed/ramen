All objects to render/edit dashboards
=====================================

Non-members of "dashboard/"
---------------------------

- TimePlot: the basic widget to render a time series

Members of "dashboard/"
-----------------------

- DashboardWindow: A mere window containing only a Dashboard.

- Dashboard: A dashboard QWidget. Dashboards are made of DashboardWidget.
  Constructed from its key prefix and updated automatically on widget
  addition/deletion/change.
  On key change that does change the widget type, perform a deletion + addition.
  On key change that does not change the widget type, merely signal the
  DashboardWidget that its confvalue has changed.

- DashboardWidget: A single widget to display a piece of (generally time
  varying) information, such as a plot. 

- DashboardWidgetText: The simplest implementation of a DashboardWidget,
  displaying a constant string.
  Constructed from its confValue (of type conf::DashboardWidgetText)

- DashboardWidgetChart: A more interesting implementation of a DashboardWidget,
  displaying a time series in a chart.
  Constructed from its confValue (of type conf::DashboardWidgetChart)