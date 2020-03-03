All objects to render charts/dashboards
=======================================

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

- PlottedField: a function output field that is being displayed, with the
  accompanying configuration, such as:
  - the factors to use (there will be one line per value of the cartesian product
    of all factors)
  - a stacking method (none, stacked, stack-centered)

- TimePlot: an AbstractTimeLine that displays a list of PlottedField objects.
  With each PlottedField is associated an opacity, an axis (left0, left1, ...,
  right0, right1...)

- TimePlotAxis: a Y axis used in TimePlot, with such configuration as the range,
  the scale_function (identity for linear scale, log for logarithmic scale...),
  whether it must be displayed on the left (with labels at left) or right (with
  labels at right) of the view...

- TimeLineGroup:

- TimeLineView
