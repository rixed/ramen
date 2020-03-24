All objects to render charts
============================

Non-members of "chart/"
-----------------------

- FunctionItem: Represents a single worker.

- PastData: Stores (and cache) all known past tuples for that workers, as a
  list of ReplayRequest objects.

- ReplayRequest: A pending or past replay request data obtained from the server.
  Tuples has all the columns.

- RollButtonDelegate: An item delegate for columns which edition consists merely
  on selecting one amongst several icons; used for the representation of field
  in time charts.

- ColorDelegate: An item delegate for color edition.

Members of "chart/"
-------------------

- AbstractTimeLine: a scrollable QWidget used to represent a chart in a given time
  range.

- HeatLine: an AbstractTimeLine that displays colored blocks.

- BinaryHeatLine: a black&white HeatLine

- TimeLine: an AbstractTimeLine that displays a time axis

- TimeLineGroup: connect together a set of AbstractTimeLine objects so that
  they scroll together.

- TimeLineView: a widget that displays an AbstractTimeLine for every defined
  function, with a TimeLine ruler on top and at the bottom.

- Ticks: The class concerned about tick marks calculations.

- TimeChart: an AbstractTimeLine that displays several time series.
  Time range is given by a slot and values are taken from a
  conf::DashWidgetChart.
  TimeChart takes actual data tuples from the iterator function of all
  involved functions (which name is obtained from the configuration).

- TimeChartEditor: Also configured from the configuration value, this one
  edits the various parameters that form a chart configuration.
  This is basically a TimeChartEditPanel and a TimeChart.

- TimeChartEditWidget: The AtomicWidget controlling what is going to be plotted
  in a TimeChart. Made of several subcomponents such as the following.

- TimeChartAxisEditor: The editor for axis parameters such as side, scale...

- TimeChartOptionsEditor: Controls the settings that apply to the TimeChart as
  a whole, such as axis configuration.

- TimeChartFunctionFieldsModel: The data model for the numeric fields of a
  given worker. Used to fill the qtableview in the TimeChartFunctionEditor.

- TimeChartFunctionsEditor: The editor that offer to choose from all involved
  functions and edit it with a TimeChartFunctionEditor.

- TimeChartFunctionEditor: The editor that controls the configuration values
  specific to a given function.
