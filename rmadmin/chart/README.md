All objects to render charts
============================

Non-members of "chart/"
-----------------------

- FunctionItem: Represents a single worker.

- PastData: Stores (and cache) all known past tuples for that workers, as a
  list of ReplayRequest objects.

- ReplayRequest: A pending or past replay request data obtained from the server.
  Tuples has all the columns.

Members of "chart/"
-------------------

- AbstractTimeLine: a scrollable QWidget used to represent a chart in a given time
  range.

- HeatLine: an AbstractTimeLine that displays colored blocks.

- BinaryHeatLine: a black&white HeatLine

- TimeLine: an AbstractTimeLine that displays a time axis

- TimeChart: an AbstractTimeLine that displays several time series.
  Time range is given by a slot and values are taken from a
  conf::DashboardWidgetChart.
  TimeChart takes actual data tuples from the iterator function of all
  involved functions (which name is obtained from the configuration).

- TimeChartEditor: Also configured from the configuration value, this one
  edits the various parameters that form a chart configuration.
  This is basically a TimeChartEditPanel and a TimeChart.

- TimeChartEditPanel: Made of several subcomponents such as the following.

- TimeChartOptionsEditor: Controls the settings that apply to the TimeChart as
  a whole. Initialized from the same conf value.

- TimeChartFunctionEditor: The editor that controls the configration values
  specific to a given function.

- TimeLineGroup: connect together a set of AbstractTimeLine objects so that
  scroll together.

- TimeLineView: a widget that displays all the AbstractTimeLine objects of a
  TimeLineGroup with a TimeLine ruler on top and at the bottom.
