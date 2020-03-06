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

- TimeChartFunctionsEditor: The editor that offer to choose from all involved
  functions and edit it with a TimeChartFunctionEditor.

- TimeChartFunctionEditor: The editor that controls the configuration values
  specific to a given function.

- TimeChartColumnEditor: The one line editor that allow to select if/how a
  column is going to be used in a chart

- TimeLineGroup: connect together a set of AbstractTimeLine objects so that
  they scroll together.

- TimeLineView: a widget that displays an AbstractTimeLine for every defined
  function, with a TimeLine ruler on top and at the bottom.
