#include <cassert>
#include <cmath>
#include <limits>
#include <QDebug>
#include <QApplication>
#include <QFont>
#include <QPainter>
#include <QPen>
#include <QPointF>
#include "chart/Ticks.h"
#include "chart/TimeChartEditWidget.h"
#include "FunctionItem.h"
#include "PastData.h"
#include "RamenType.h"
#include "RamenValue.h"
#include "TailModel.h"

#include "chart/TimeChart.h"

static bool const verbose = true;

static double const defaultBeginOftime = 0.;
static double const defaultEndOfTime = 600.;

static int tickLabelWidth = 50;
static int tickLabelHeight = 15;
static int minPlotWidth = 300;

TimeChart::TimeChart(TimeChartEditWidget *editWidget_, QWidget *parent)
  : AbstractTimeLine(defaultBeginOftime, defaultEndOfTime, true, true, parent),
    editWidget(editWidget_)
{
  setMinimumSize(tickLabelWidth*2 + minPlotWidth, tickLabelHeight*5);

  /* Connect to signals from the editWidget:
   * - for axis, a signal when any axis config is changed
   * - for fields, a signal when any field config is changed */
  connect(editWidget, &TimeChartEditWidget::axisChanged,
          this, &TimeChart::redrawAxis);
  connect(editWidget, &TimeChartEditWidget::fieldChanged,
          this, &TimeChart::redrawField);
}

std::optional<int> TimeChart::anyAxis(bool left) const
{
  for (int i = 0; i < editWidget->axisCount(); i++) {
    std::optional<conf::DashboardWidgetChart::Axis> axis = editWidget->axis(i);
    assert(axis);
    if (axis->left == left) return i;
  }

  return std::nullopt;
}

void TimeChart::redrawAxis(int axisNum)
{
  if (verbose)
    qDebug() << "TimeChart::redrawAxis" << axisNum;

  std::optional<conf::DashboardWidgetChart::Axis const> axis =
    editWidget->axis(axisNum);
  if (axis) {
    // If the axis switched side, remove it from the old side:
    if (axis->left && focusedAxis[Right] == axisNum) {
      focusedAxis[Right] = anyAxis(false);
    } else if (!axis->left && focusedAxis[Left] == axisNum) {
      focusedAxis[Left] = anyAxis(true);
    }

    // In any case, focus that axis on its side, and take control of the grid:
    if (axis->left) {
      focusedAxis[Left] = focusedGridAxis = axisNum;
    } else {
      focusedAxis[Right] = focusedGridAxis = axisNum;
    }

    update();

  } else {
    // Axis is no more: remove it from focus.
    if (focusedAxis[Left] == axisNum) {
      focusedAxis[Left] = anyAxis(true);
      if (focusedGridAxis == axisNum)
        focusedGridAxis = focusedAxis[Left];

      update();

    } else if (focusedAxis[Right] == axisNum) {
      focusedAxis[Right] = anyAxis(false);
      if (focusedGridAxis == axisNum)
        focusedGridAxis = focusedAxis[Right];

      update();
    }
  }
}

void TimeChart::redrawField(
  std::string const &site, std::string const &program,
  std::string const &function, std::string const &name)
{
  if (verbose)
    qDebug() << "TimeChart::redrawField"
             << QString::fromStdString(site)
             << QString::fromStdString(program)
             << QString::fromStdString(function)
             << QString::fromStdString(name);

  // Make that field the focused one
  focusedField = FieldFQ({ site, program, function, name });

  update();
}

/* Given v_ratio = (v-min)/(max-min), return the Y pixel coordinate: */
qreal TimeChart::YofV(qreal v, qreal min, qreal max, bool log, int base) const
{
  if (log) {
    v = logOfBase(base, v);
    min = logOfBase(base, min);
    max = logOfBase(base, max);
  }

  return (1 - (v - min)/(max - min)) * height();
}

qreal TimeChart::VofY(int y, qreal min, qreal max, bool log, int base) const
{
  if (log) {
    min = logOfBase(base, min);
    max = logOfBase(base, max);
  }

  qreal v(
    (1 - static_cast<qreal>(y)/height()) * (max - min) + min);

  if (log) {
    return std::pow(base, v);
  } else {
    return v;
  }
}

static std::pair<bool, int> get_log_base(
  std::optional<conf::DashboardWidgetChart::Axis const> const &conf)
{
  if (conf) {
    switch (conf->scale) {
      case conf::DashboardWidgetChart::Axis::Linear:
        break;  // default
      case conf::DashboardWidgetChart::Axis::Logarithmic:
        return std::make_pair(true, 10);
      /* TODO: Later when units have their own scales we could
       * choose another base (such as 2 or 8 for data volumes,
       * 60 for durations...) to pick the ticks */
    }
  }
  return std::make_pair(false, 10);
}

void TimeChart::paintGrid(
  Axis const &axis,
  std::map<FieldFQ, PerFunctionResults> &)
{
  QPainter painter(this);
  painter.setRenderHint(QPainter::Antialiasing);
  painter.setRenderHint(QPainter::TextAntialiasing);
  QColor const bgColor(palette().color(QWidget::backgroundRole()));
  QColor const gridColor(bgColor.lighter(120));

  if (axis.min >= axis.max) {
    QPen pen(gridColor, 1);
    painter.setPen(pen);
    QFont font(painter.font());
    font.setPixelSize(tickLabelHeight);
    painter.drawText(rect(), Qt::AlignCenter, "no data");
    return;
  }

  QPen majorPen(gridColor, 1.5, Qt::SolidLine);
  QPen minorPen(gridColor, 1, Qt::DashLine);

  std::pair<bool, int> log_base = get_log_base(axis.conf);
  Ticks ticks(axis.min, axis.max, log_base.first, log_base.second);

  int const x1(0/*tickLabelWidth*/);
  int const x2(width()/* - tickLabelWidth*/);
  for (struct Tick const &tick : ticks.ticks) {
    /*qDebug() << (tick.major ? "major" : "minor") << "tick at"
             << tick.pos << ":" << tick.label;*/

    painter.setPen(tick.major ? majorPen : minorPen);
    qreal const y(
      YofV(tick.pos, axis.min, axis.max, log_base.first, log_base.second));
    if (! std::isnan(y)) painter.drawLine(x1, y, x2, y);
  }
}

void TimeChart::paintTicks(
  Side const side,
  Axis const &axis,
  std::map<FieldFQ, PerFunctionResults> &)
{
  if (axis.min >= axis.max) return;

  QPainter painter(this);
  painter.setRenderHint(QPainter::Antialiasing);
  painter.setRenderHint(QPainter::TextAntialiasing);
  QColor const tickColor(palette().color(QPalette::Text));

  painter.setPen(tickColor);
  QFont majorFont(painter.font());
  majorFont.setPixelSize(tickLabelHeight);
  QFont minorFont(painter.font());
  minorFont.setPixelSize(tickLabelHeight*2/3);

  std::pair<bool, int> log_base = get_log_base(axis.conf);
  Ticks ticks(axis.min, axis.max, log_base.first, log_base.second);
  int const x2(width() - tickLabelWidth);
  for (struct Tick const &tick : ticks.ticks) {
    painter.setFont(tick.major ? majorFont : minorFont);
    qreal const y(
      YofV(tick.pos, axis.min, axis.max, log_base.first, log_base.second));
    if (!std::isnan(y)) {
      if (side == Left) {
        painter.drawText(
          QRect(0, y - tickLabelHeight/2, tickLabelWidth, tickLabelHeight),
          Qt::AlignLeft, tick.label);
      } else {
        assert(side == Right);
        painter.drawText(
          QRect(x2, y - tickLabelHeight/2, tickLabelWidth, tickLabelHeight),
          Qt::AlignRight, tick.label);
      }
    }
  }
}

void TimeChart::paintAxis(
  Axis &axis,
  std::map<FieldFQ, PerFunctionResults> &funcs)
{
  if (axis.min >= axis.max) return;

  std::pair<bool, int> log_base = get_log_base(axis.conf);

  QPainter painter(this);
  painter.setRenderHint(QPainter::Antialiasing);
  painter.setRenderHint(QPainter::TextAntialiasing);

  // Draw each independent fields
  for (Line const &line : axis.independent) {
    auto const &it(funcs.find(line.ffq));
    if (it == funcs.end()) continue; // should not happen
    PerFunctionResults const &res = it->second;

    painter.setPen(line.color);

    if (res.noEventTime) {
      QFont font(painter.font());
      font.setPixelSize(tickLabelHeight);
      painter.drawText(rect(), Qt::AlignCenter, "no event-time information");
      continue;
    }

    std::optional<QPointF> last; // the last point
    bool lastDrawn(false);  // was the last point drawn somehow?
    for (size_t i = 0; i < res.tuples.size(); i++) {
      std::optional<double> const v(res.tuples[i].second[line.columnIndex]);
      if (v) {
        qreal const y(
          YofV(*v, axis.min, axis.max, log_base.first, log_base.second));
        if (! std::isnan(y)) {
          QPointF cur(toPixel(res.tuples[i].first), y);
          if (last)
            painter.drawLine(*last, cur);
          lastDrawn = (bool)last;
          last = cur;
        } else {
          last = std::nullopt;
        }
      } else {
        if (last && !lastDrawn)
          painter.drawPoint(*last); // Probably a larger pen is in order
        last = std::nullopt;
      }
    }
    if (last && !lastDrawn)
      painter.drawPoint(*last);
  }

  // Draw stacked lines.
  std::function<void(std::vector<Line> const &, bool)> drawStacked =
    [this, &axis, &funcs, log_base](std::vector<Line> const &lines, bool center) {
      QPainter painter(this);
      size_t const numLines(lines.size());
      std::optional<QPointF> last[numLines];
      qreal lastOffs[numLines];
      qreal lastX(0);
      qreal const zeroY(
        YofV(0, axis.min, axis.max, log_base.first, log_base.second));
      for (size_t l = 0; l < numLines; l++) {
        lastOffs[l] = zeroY;
      }
      Axis::iterTime(funcs, lines,
        [this, &axis, &lines, &painter, numLines, log_base, &last, &lastOffs,
         &lastX, center](
          double time, std::optional<qreal> values[]) {
            qreal totVal(0.);
            if (center) {
              for (size_t l = 0; l < numLines; l++)
                if (values[l]) totVal += *values[l];
            }
            qreal const x(toPixel(time));
            qreal tot(-totVal/2);
            qreal prevY(
              YofV(tot, axis.min, axis.max, log_base.first, log_base.second));
            for (size_t l = 0; l < numLines; l++) {
              if (values[l]) {
                if (verbose && *values[l] < 0)
                  qDebug() << "Stacked chart with negative values";
                qreal const y(
                  YofV(*values[l] + tot,
                       axis.min, axis.max, log_base.first, log_base.second));
                if (! std::isnan(y)) {
                  QPointF cur(x, y);
                  if (last[l]) {
                    QPointF const points[4] = {
                      *last[l],
                      QPointF(lastX, lastOffs[l]),
                      QPointF(x, prevY),
                      cur
                    };
                    painter.setPen(Qt::NoPen);
                    QColor fill(lines[l].color);
                    fill.setAlpha(100);
                    painter.setBrush(fill);
                    painter.setRenderHint(QPainter::Antialiasing, false);
                    painter.drawConvexPolygon(points, 4);
                    painter.setPen(lines[l].color);
                    painter.setRenderHint(QPainter::Antialiasing);
                    painter.drawLine(*last[l], cur);
                  }
                  last[l] = cur;
                  lastOffs[l] = prevY;
                  prevY = y;
                  tot += *values[l];
                } else {
                  last[l] = std::nullopt;
                }
              } else {
                last[l] = std::nullopt;
              }
            }
            lastX = x;
      });
  };

  drawStacked(axis.stacked, false);
  drawStacked(axis.stackCentered, true);
}

static int getFieldNum(
  std::shared_ptr<Function const> func, std::string const &fieldName_)
{
  // Prepare the field number in the function output type:
  std::shared_ptr<RamenType const> outType(func->outType());
  if (! outType) {
    qWarning() << "TimeChart: Cannot find outType for" << func->fqName;
    return -1;
  }
  QString const fieldName(QString::fromStdString(fieldName_));
  for (int c = 0; c < outType->structure->numColumns(); c++) {
    QString const columnName(outType->structure->columnName(c));
    if (columnName == fieldName) return c;
  }
  qWarning() << "TimeChart: Cannot find field number for" << fieldName;
  return -1;
}

void TimeChart::paintEvent(QPaintEvent *event)
{
  AbstractTimeLine::paintEvent(event);

  /* Start by collecting all the points per axis: */
  int const numAxes = editWidget->axisCount();

  /* For each axis, remember the field name and which position its
   * value will be in the result columns vector */
  std::vector<Axis> axes;
  for (int i = 0; i < numAxes; i++) {
    axes.emplace_back(editWidget->axis(i));
  }

  /* Cache the required function and the columns we need from them: */
  std::map<FieldFQ, PerFunctionResults> funcs;

  /* First, collect the Function pointer and columns vectors (ie fill
   * the above funcs map) */
  editWidget->iterFields([this,numAxes,&funcs,&axes](
    std::string const &site, std::string const &program,
    std::string const &function, conf::DashboardWidgetChart::Column const &field) {

    if (verbose)
      qDebug() << "TimeChart: Preparing collections for field"
               << QString::fromStdString(field.name);

    assert(field.axisNum >= 0);
    if (field.axisNum >= numAxes) {
      /* Can happen in between a new axis is used in the fields and that axis
       * being added to the axis editor. */
      qDebug() << "Field uses axis" << field.axisNum
               << "that's not been created yet";
      return;
    }

    // Lookup the function:
    std::shared_ptr<Function> func;
    FieldFQ const ffq({ site, program, function, field.name });
    auto it(funcs.find(ffq));
    if (it != funcs.end()) {
      func = it->second.func;
    } else {
      func = Function::find(QString::fromStdString(site),
                            QString::fromStdString(program),
                            QString::fromStdString(function));
      if (! func) {
        qCritical("TimeChart: Cannot find Function for %s:%s/%s",
          site.c_str(), program.c_str(), function.c_str());
        return; // better safe than sorry
      }
      if (verbose)
        qDebug() << "TimeChart: Will need function" << func->fqName;
      auto emplaced = funcs.emplace(ffq, func);
      it = emplaced.first;

      /* Also ask for this function's tails: */
      std::shared_ptr<TailModel const> tailModel(func->getTail());
      if (! tailModel) {
        if (verbose)
          qDebug() << "TimeChart: Requesting tail";
        tailModel = func->getOrCreateTail();
        if (tailModel) {
          connect(tailModel.get(), &TailModel::rowsInserted, [this]() {
              update();
          });
        } else {
          qCritical() << "TimeChart: Cannot get tail for function" << func->fqName;
        }
      }
    }
    assert(func);
    int const fieldNum(getFieldNum(func, field.name));
    if (fieldNum < 0) return;

    // Add this field in the request and remember the location of this field:
    PerFunctionResults &res = it->second;
    switch (field.representation) {
      case conf::DashboardWidgetChart::Column::Unused:
        break;  // Well tried!
      case conf::DashboardWidgetChart::Column::Independent:
        axes[field.axisNum].independent.emplace_back(
          ffq, res.columns.size(), field.color);
        break;
      case conf::DashboardWidgetChart::Column::Stacked:
        axes[field.axisNum].stacked.emplace_back(
          ffq, res.columns.size(), field.color);
        break;
      case conf::DashboardWidgetChart::Column::StackCentered:
        axes[field.axisNum].stackCentered.emplace_back(
          ffq, res.columns.size(), field.color);
        break;
    }

    if (verbose)
      qDebug() << "TimeChart: field" << QString::fromStdString(field.name)
               << "at position" << res.columns.size();
    res.columns.push_back(fieldNum);
  });

  // Then iterate over all functions and fill in the results with actual tuples:
  for (auto &it : funcs) {
    PerFunctionResults &res = it.second;
    // Can happen if fieldNum could not be found:
    if (res.columns.empty()) continue;

    std::shared_ptr<EventTime const> eventTime(res.func->getTime());
    if (! eventTime) {
      res.noEventTime = true;
      continue;
    }

    if (verbose)
      qDebug() << "TimeChart: collecting tuples for" << res.columns.size()
               << "columns of" << res.func->fqName
               << "between" << m_viewPort.first << "and" << m_viewPort.second;

    res.func->iterValues(m_viewPort.first, m_viewPort.second, true, res.columns,
      [&res](double time, std::vector<RamenValue const *> const values) {
      std::pair<double, std::vector<std::optional<double>>> &tuple(
        res.tuples.emplace_back());
      tuple.first = time;
      tuple.second.reserve(values.size());
      assert(values.size() == 1);
      for (size_t i = 0; i < values.size(); i++) {
        tuple.second.push_back(values[i]->toDouble());
      }
    });

    if (verbose)
      qDebug() << "TimeChart: got" << res.tuples.size() << "tuples";
  }

  /* Compute the extremums per axis: */
  for (int i = 0; i < numAxes; i++) {
    Axis &axis(axes[i]);
    std::function<void(qreal const min, qreal const max)> updateExtremums =
      [&axis](qreal const min, qreal const max) {
        if (min < axis.min) axis.min = min;
        if (max > axis.max) axis.max = max;
      };

    if (axis.conf && axis.conf->forceZero) updateExtremums(0., 0.);

    for (auto &line : axis.independent) {
      auto const &it(funcs.find(line.ffq));
      if (it == funcs.end()) continue;
      PerFunctionResults const &res(it->second);
      // Get the min/max over the whole time range:
      for (std::pair<double, std::vector<std::optional<double>>> const &tuple :
             res.tuples) {
        std::optional<double> const &v(tuple.second[line.columnIndex]);
        if (v) updateExtremums(*v, *v);
      }
    }

    /* Set extremums for stacked lines: */
    Axis::iterTime(funcs, axis.stacked, [updateExtremums,&axis](
      double, std::optional<qreal> values[]) {
        qreal totHeight(0.);
        for (size_t i = 0; i < axis.stacked.size(); i++) {
          if (values[i]) totHeight += *values[i];
        }
        updateExtremums(0., totHeight);
    });

    /* Set extremums for stack-centered lines: */
    Axis::iterTime(funcs, axis.stackCentered, [updateExtremums,&axis](
      double, std::optional<qreal> values[]) {
        qreal totHeight(0.);
        for (size_t i = 0; i < axis.stackCentered.size(); i++) {
          if (values[i]) totHeight += *values[i];
        }
        updateExtremums(-totHeight/2, totHeight/2);
    });

  }

  /* If no axis is focused, focus the first left and right ones: */
  if (! focusedAxis[Left])
    focusedAxis[Left] = editWidget->firstAxisOnSide(true);
  if (! focusedAxis[Right])
    focusedAxis[Right] = editWidget->firstAxisOnSide(false);
  if (! focusedGridAxis)
    focusedGridAxis = focusedAxis[Left] ?
      focusedAxis[Left] : focusedAxis[Right];

  if (focusedGridAxis) {
    assert(*focusedGridAxis < numAxes);
    paintGrid(axes[*focusedGridAxis], funcs);
  }

  if (focusedAxis[Left]) {
    assert(*focusedAxis[Left] < numAxes);
    paintTicks(Left, axes[*focusedAxis[Left]], funcs);
  }

  if (focusedAxis[Right]) {
    assert(*focusedAxis[Right] < numAxes);
    paintTicks(Right, axes[*focusedAxis[Right]], funcs);
  }

  /* Draw all axes but the main one: */
  for (int i = 0; i < numAxes; i++) {
    if (focusedGridAxis && i == *focusedGridAxis)
      continue; // keep this for later

    paintAxis(axes[i], funcs);
  }

  // Finally, the focused one:
  if (focusedGridAxis)
    paintAxis(axes[*focusedGridAxis], funcs);
}

void TimeChart::Axis::iterTime(
  std::map<FieldFQ, PerFunctionResults> const &funcs,
  std::vector<Line> const &lines,
  std::function<void(double, std::optional<qreal>[])> cb)
{
  if (lines.size() == 0) return;

  size_t const numLines(lines.size());
  size_t indices[numLines];
  PerFunctionResults const *res[numLines];
  for (size_t l = 0; l < numLines; l++) {
    indices[l] = 0;
    auto const &it(funcs.find(lines[l].ffq));
    assert (it != funcs.end());
    res[l] = &it->second;
  }

  bool allDone;
  while (true) {
    // Get the smaller next time:
    allDone = true;
    double time(std::numeric_limits<double>::max());
    for (size_t l = 0; l < numLines; l++) {
      if (indices[l] < res[l]->tuples.size() &&
          res[l]->tuples[indices[l]].first < time) {
        time = res[l]->tuples[indices[l]].first;
        allDone = false;
      }
    }

    if (allDone) break;

    // Build the values
    std::optional<qreal> values[numLines];
    for (size_t l = 0; l < numLines; l++) {
      if (indices[l] < res[l]->tuples.size() &&
          res[l]->tuples[indices[l]].first == time) {
        values[l] = res[l]->tuples[indices[l]++].second[lines[l].columnIndex];
      } else {
        values[l] = std::nullopt;
      }
    }

    cb(time, values);

  }
}

bool FieldFQ::operator<(FieldFQ const &o) const
{
  int const cs(site.compare(o.site));
  if (cs < 0) return true;
  else if (cs > 0) return false;
  int const cp(program.compare(o.program));
  if (cp < 0) return true;
  else if (cp > 0) return false;
  int const cf(function.compare(o.function));
  if (cf < 0) return true;
  else if (cf > 0) return false;
  int const cn(name.compare(o.name));
  return cn < 0;
}

/*
bool FieldFQ::operator<(FieldFQ const &o) const
{
  bool res = lessThan(o);
  if (res)
    qDebug() << "FieldFQ:" << *this << "<" << o;
  else
    qDebug() << "FieldFQ:" << o << ">=" << *this;
  return res;
}
*/

QDebug operator<<(QDebug dbg, FieldFQ const &ffq)
{
  QDebugStateSaver saver(dbg);
  dbg.nospace()
    << QString::fromStdString(ffq.site) << ":"
    << QString::fromStdString(ffq.program) << "/"
    << QString::fromStdString(ffq.function) << "["
    << QString::fromStdString(ffq.name) << "]";
  return dbg;
}
