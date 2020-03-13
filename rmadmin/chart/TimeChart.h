#ifndef TIMECHART_H_200203
#define TIMECHART_H_200203
#include <optional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <QDebug>
#include "AbstractTimeLine.h"
#include "confValue.h"

/* A TimeChart diplay the actual plot.
 * It is configured from a TimeChartWidget that it provided in the constructor,
 * to which it attaches to receive the various changes. */

class Function;
class QPaintEvent;
class TimeChartEditWidget;

/* Similarly, there is a focused field (which axis should also be focused).
 * When field name is empty then no field is focused and other strings are
 * undefined. */
struct FieldFQ {
  std::string site, program, function, name;

  bool operator<(FieldFQ const &) const;
};

QDebug operator<<(QDebug, FieldFQ const &);

class TimeChart : public AbstractTimeLine
{
  Q_OBJECT

  TimeChartEditWidget *editWidget;

  /* We only ever display one axis at a time on each side, and one axis
   * has the control of the background grid. */
  enum Side { Left, Right, NumSide };
  std::optional<int> focusedAxis[NumSide], focusedGridAxis;

  /* Result of a function iteration: */
  struct PerFunctionResults {
    std::shared_ptr<Function> func; // the function to get data from
    std::vector<int> columns; // what columns are we interested in
    // Time, then tuple:
    std::vector<std::pair<double, std::vector<std::optional<double>>>> tuples;
    // Set when the tuples are missing because of missing event Time:
    bool noEventTime;

    PerFunctionResults(std::shared_ptr<Function> func_)
      : func(func_), noEventTime(false) {
      columns.reserve(5);
      tuples.reserve(500);
    }
  };

  FieldFQ focusedField;

  /* An individual line representing one field: */
  struct Line {
    FieldFQ ffq;
    size_t columnIndex; // where is this value in the result solumns
    QColor color;

    Line(FieldFQ const &ffq_, size_t ci, QColor const &co)
      : ffq(ffq_), columnIndex(ci), color(co) {}
  };

  struct Axis {
    /* Each axis has 3 distinct, possibly empty sets of plots:
     * - a stack plot;
     * - a stack-centered plot;
     * - a set of independent fields. */
    std::vector<Line> stacked;
    std::vector<Line> stackCentered;
    std::vector<Line> independent;

    /* extremums of all the plots/stacks.
     * This is the min/max of the global resulting picture, used to draw the
     * axis tick marks. */
    qreal min, max;

    std::optional<conf::DashboardWidgetChart::Axis const> conf;

    Axis(std::optional<conf::DashboardWidgetChart::Axis const> conf_)
      : min(std::numeric_limits<qreal>::max()),
        max(std::numeric_limits<qreal>::min()),
        conf(conf_) {
      stacked.reserve(10);
      stackCentered.reserve(10);
      independent.reserve(10);
    }
  };

  void paintGrid(
    Axis const &,
    std::map<FieldFQ, PerFunctionResults> &);

  void paintTicks(
    Side const,
    Axis const &,
    std::map<FieldFQ, PerFunctionResults> &);

  void paintAxis(
    Axis const &,
    std::map<FieldFQ, PerFunctionResults> &);

public:
  TimeChart(TimeChartEditWidget *editWidget, QWidget *parent = nullptr);

protected:
  std::optional<int> anyAxis(bool) const;

  void paintEvent(QPaintEvent *) override;

  qreal YofV(qreal v, qreal min, qreal max, bool log, int base) const;

  qreal VofY(int y, qreal min, qreal max, bool log, int base) const;

protected slots:
  /* Focus this axis and redraw it: */
  void redrawAxis(int);

  /* Focus this field and redraw it: */
  void redrawField(
    std::string const &site, std::string const &program,
    std::string const &function, std::string const &field);
};

#endif
