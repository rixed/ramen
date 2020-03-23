#ifndef TIMECHART_H_200203
#define TIMECHART_H_200203
#include <optional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <QDebug>
#include <QString>
#include "AbstractTimeLine.h"
#include "confValue.h"

/* A TimeChart diplay the actual plot.
 * It is configured from a TimeChartWidget that it provided in the constructor,
 * to which it attaches to receive the various changes. */

class Function;
class QPaintEvent;
class TimeChartEditWidget;

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

  struct FieldFQ {
    QString funcFq;
    std::string name;
  } focusedField;

  /* An individual line representing one field: */
  struct Line {
    /* Lines are build once funcs map is build and PerFunctionResults cannot
     * be moved in memory any longer: */
    PerFunctionResults const *res;
    std::string fieldName;
    size_t columnIndex; // where is this value in the result columns
    QColor color;

    Line(PerFunctionResults const *res_, std::string const fieldName_,
         size_t ci, QColor const &co)
      : res(res_), fieldName(fieldName_), columnIndex(ci), color(co) {}
  };

  struct Axis {
    /* Each axis has 3 distinct, possibly empty sets of plots:
     * - a set of stack plot (one stack per function);
     * - a set of stack-centered plot (one per function);
     * - a set of independent fields. */
    std::vector<Line> stacked;
    std::vector<Line> stackCentered;
    std::vector<Line> independent;

    /* Given a set of lines and a func, call the given function for every time
     * step with the value (and color) for each lines of this func: */
    static void iterTime(
      PerFunctionResults const &res,
      std::vector<Line> const &lines,
      std::function<void(
        double,
        std::vector<std::pair<std::optional<qreal>, QColor const &>>)>);

    /* extremums of all the plots/stacks.
     * This is the min/max of the global resulting picture, used to draw the
     * axis tick marks. */
    qreal min, max;

    std::optional<conf::DashWidgetChart::Axis const> conf;

    Axis(std::optional<conf::DashWidgetChart::Axis const> conf_)
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
    std::map<QString, PerFunctionResults> &);

  void paintTicks(
    Side const,
    Axis const &,
    std::map<QString, PerFunctionResults> &);

  void paintAxis(
    Axis &,
    std::map<QString, PerFunctionResults> &);

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

signals:
  void newTailTime(double);
};

#endif
