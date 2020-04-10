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

  /* Possible values for a combination of factor: */
  struct FactorValues {
    // factor name * factor index in columns:
    std::vector<std::pair<std::string, int>> indices;
    /* Possible values, as a map from the label to all tuple indices (ordered)
     * with this label: */
    std::map<QString, std::vector<size_t>> labels;

    FactorValues(std::vector<std::pair<std::string, int>> indices_)
      : indices(indices_) {}
  };

  struct CategorizedTuple {
    double time;
    std::vector<std::optional<double>> values;

    CategorizedTuple(double time_, std::vector<RamenValue const *> const tuple)
      : time(time_)
    {
      /* Convert the whole tuple into a vector of doubles.
       * Not very useful for the factors but that's probably faster than
       * trying to distinguish them. */
      values.reserve(tuple.size());
      for (size_t i = 0; i < tuple.size(); i++) {
        values.push_back(tuple[i]->toDouble());
      }
    }

    QString toQString() const {
      QStringList res;
      for (std::optional<double> const &v : values)
        res += v ? QString::number(*v) : QString("null");
      return "[" + res.join(",") + "]";
    }
  };

  /* Result of a function iteration: */
  struct PerFunctionResults {
    std::shared_ptr<Function> func; // the function to get data from
    std::vector<int> columns; // what columns are we interested in
    std::vector<CategorizedTuple> tuples;
    // What combinations of factors are we interested in:
    std::vector<FactorValues> factorValues;
    // Set when the tuples are missing because of missing event Time:
    bool noEventTime;

    PerFunctionResults(std::shared_ptr<Function> func_)
      : func(func_), noEventTime(false) {
      columns.reserve(5);
      tuples.reserve(500);
      factorValues.reserve(3);
    }

    /* Ask for the possible values for this combination of factors and return
     * the index of that set in factorValues: */
    size_t addFactors(std::vector<std::string> const &);
  };

  struct FieldFQ {
    QString funcFq;
    std::string name;
  } focusedField;

  /* An individual line representing one field or a collection of such lines
   * if spread across factors: */
  struct Line {
    /* Lines are build once funcs map is build and PerFunctionResults cannot
     * be moved in memory any longer: */
    PerFunctionResults const *res;
    std::string fieldName;
    // Where is this value in the result columns:
    size_t columnIndex;
    // index of the factor combination used in PerFunctionResult::factorValues
    size_t factorValues;
    QColor color;

    Line(PerFunctionResults const *res_, std::string const fieldName_,
         size_t ci, size_t fi, QColor const &co)
      : res(res_), fieldName(fieldName_),
        columnIndex(ci), factorValues(fi), color(co) {}
  };

  struct Axis {
    /* Each axis has 3 distinct, possibly empty sets of plots:
     * - a set of stack plot (one stack per function);
     * - a set of stack-centered plot (one per function);
     * - a set of independent fields. */
    std::vector<Line> stacked;
    std::vector<Line> stackCentered;
    std::vector<Line> independent;

    /* Given a set of lines, call the given function for every time
     * step with the value (and color) for each lines (inc. per factor): */
    static void iterTime(
      std::vector<Line> const &lines,
      std::function<void(
        double,
        std::vector<std::pair<std::optional<qreal>, QColor>>)>);

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

    /* If no line using this Axis has a valid event time (used to output
     * a better error message) */
    bool hasEventTime() const;
  };

  void paintGrid(Axis const &);

  void paintTicks(
    Side const,
    Axis const &,
    std::map<QString, PerFunctionResults> &);

  void paintAxis(Axis const &);

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
