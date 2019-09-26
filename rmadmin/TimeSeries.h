#ifndef TIMESERIES_190925
#define TIMESERIES_190925
#include <QSharedPointer>
#include "Graphic.h"

class QCustomPlot;
class QCheckBox;
class QCPAxisTickerDateTime;

/* A graph to display timeseries.
 * TODO: Inherit a XYPlot that does not force X to be a date. */
class TimeSeries : public Graphic
{
  Q_OBJECT

  QCustomPlot *plot;

  // Total span over all points:
  double xMin, xMax, yMin, yMax;

  /* Time will always be represented on the X axis.
   *
   * We just needs to know what dataset is X and its scale.
   * FIXME: we should not have to select time excplicitely: the
   * TimeSeries graphic should be available as soon as 1 numeric
   * column is selected, and it should get the time itself from the
   * chart, which compute it from the table and function info directly.
   */
  // TODO: Autodetect which dataset has time and store it here:
  int xDataset;
  // TODO: Get this from the event time definition
  double timeUnit;

  /* Some numeric data will always be represented on either the left
   * or right axis.
   * Non numeric data can have another role: that of a factor to categorise
   * all numeric values.
   * It is then possible to choose which column is to be scaled against
   * the right axis, and which column is to be used as factor for each
   * numeric column.
   * For now this is fixed:
   * First selected column is time, second is data for the left axis,
   * third is factor for that data, fourth is data for the right axis,
   * fifth is factor for that data. */
  int y1Dataset, y2Dataset; // -1 for unassigned
  int factor1, factor2; // -1 for unassigned

  // When checked, force 0 to belong to [yMin;yMax]:
  QCheckBox *forceZeroCheckBox;


  bool addPoints(unsigned first = 0);

  // All TimeSeries share the same date ticker:
  static QSharedPointer<QCPAxisTickerDateTime> dateTicker;

public:
  TimeSeries(Chart *);
  void update() const;

protected slots:
  /* Plot the new points that's been added to the chart datasets since last
   * time: */
  void appendValues();
  /* Keep the same data but update the presentation: */
  void reformat();
};

#endif
