#include <cassert>
#include <cmath>
#include <limits>
#include <mutex>
#include <QDebug>
#include <QApplication>
#include <QBrush>
#include <QFont>
#include <QPainter>
#include <QPen>
#include <QPointF>
#include <QStaticText>
#include "chart/Ticks.h"
#include "chart/TimeChartEditWidget.h"
#include "FunctionItem.h"
#include "misc.h"
#include "PastData.h"
#include "RamenType.h"
#include "RamenValue.h"
#include "TailModel.h"

#include "chart/TimeChart.h"

static bool const verbose(false);

static double const defaultBeginOftime = 0.;
static double const defaultEndOfTime = 600.;

static int const tickLabelWidth(50);
static int const tickLabelHeight(15);
static int const minPlotWidth(300);
static int const metaFontHeight(18);

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
  QString const funcFq(
    QString::fromStdString(site) + ":" +
    QString::fromStdString(program) + "/" +
    QString::fromStdString(function));

  if (verbose)
    qDebug() << "TimeChart::redrawField"
             << funcFq
             << QString::fromStdString(name);

  // Make that field the focused one
  focusedField = FieldFQ({ funcFq, name });

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
    return sameSign(v, std::pow(base, std::abs(v)));
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
  std::map<QString, PerFunctionResults> &)
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
    painter.setFont(font);
    painter.drawText(rect(), Qt::AlignCenter, "no data");
    return;
  }

  QPen majorPen(gridColor, 1.5, Qt::SolidLine);
  QPen minorPen(gridColor, 1, Qt::DashLine);

  std::pair<bool, int> log_base(get_log_base(axis.conf));
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
  std::map<QString, PerFunctionResults> &)
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
  std::map<QString, PerFunctionResults> &funcs)
{
  if (axis.min >= axis.max) return;

  std::pair<bool, int> log_base = get_log_base(axis.conf);

  QPainter painter(this);
  painter.setRenderHint(QPainter::Antialiasing);
  painter.setRenderHint(QPainter::TextAntialiasing);

  std::time_t const now(getTime());

  // Draw each independent fields
  for (Line const &line : axis.independent) {
    if (line.res->noEventTime) {
      QFont font(painter.font());
      font.setPixelSize(tickLabelHeight);
      painter.setFont(font);
      painter.setPen(line.color);
      painter.drawText(rect(), Qt::AlignCenter, "no event-time information");
      continue;
    }

    QColor const darkCol(line.color.darker());

    /* Draw some meta informations: ongoing queries, where the tail begin. */
    std::shared_ptr<TailModel> tail(line.res->func->getTail());
    QFont font(painter.font());
    font.setPixelSize(metaFontHeight);
    painter.setFont(font);
    if (tail) {
      qreal const x(toPixel(tail->minEventTime()));
      QPen pen(darkCol);
      pen.setStyle(Qt::DashDotLine);
      painter.setPen(pen);
      painter.drawLine(x, 0, x, height());
      painter.setPen(line.color);
      painter.rotate(-90);
      static const QStaticText tailText("Tail...");
      // Possibly: display tail size
      painter.drawStaticText(-height(), x, tailText);
      painter.rotate(90);
    }
    std::shared_ptr<PastData> past(line.res->func->getPast());
    if (past) {
      for (ReplayRequest &replay : past->replayRequests) {
        qreal x1, x2;
        size_t numTuples;
        QColor dimCol(darkCol);
        {
          std::lock_guard<std::mutex> guard(replay.lock);
          if (replay.isCompleted(guard)) {
            // Dim the color with age
            double const age(now - replay.started);
            double const maxAge(20);
            if (age < 0 || age > maxAge) continue;
            dimCol = blendColor(dimCol, painter.background().color(),
                                age / maxAge);
          }
          x1 = toPixel(replay.since);
          x2 = toPixel(replay.until);
          numTuples = replay.tuples.size();
        }
        QBrush const brush(dimCol, Qt::BDiagPattern);
        painter.setBrush(brush);
        QRect const r(x1, 0, x2 - x1, height());
        QPen pen(dimCol);
        pen.setStyle(Qt::DashDotLine);
        painter.setPen(pen);
        painter.drawRect(r);
        painter.rotate(-90);
        painter.drawText(
          -height(), x1,
          QString::number(numTuples) + QString(" tuples"));
        painter.rotate(90);
      }
    }

    painter.setPen(line.color);
    std::optional<QPointF> last; // the last point
    bool lastDrawn(false);  // was the last point drawn somehow?
    for (size_t i = 0; i < line.res->tuples.size(); i++) {
      std::optional<double> const v(line.res->tuples[i].second[line.columnIndex]);
      if (v) {
        qreal const y(
          YofV(*v, axis.min, axis.max, log_base.first, log_base.second));
        if (! std::isnan(y)) {
          QPointF cur(toPixel(line.res->tuples[i].first), y);
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
      qreal const zeroY(
        YofV(0, axis.min, axis.max, log_base.first, log_base.second));

      // Draw one stack per function:
      for (auto &it : funcs) {
        PerFunctionResults &res = it.second;

        size_t const numLines(lines.size()); // at most, using this function
        std::optional<QPointF> last[numLines];
        qreal lastOffs[numLines];
        for (size_t l = 0; l < numLines; l++) {
          lastOffs[l] = zeroY;
        }
        qreal lastX(0);

        Axis::iterTime(res, lines,
          [this, &axis, &painter, log_base, &last, &lastOffs,
           &lastX, center](
            double time,
            std::vector<std::pair<std::optional<qreal>, QColor const &>> values) {
              qreal totVal(0.);
              if (center) {
                for (auto const &value : values)
                  if (value.first) totVal += *value.first;
              }
              qreal const x(toPixel(time));
              qreal tot(-totVal/2);
              qreal prevY(
                YofV(tot, axis.min, axis.max, log_base.first, log_base.second));
              for (size_t l = 0; l < values.size(); l++) {
                if (values[l].first) {
                  if (verbose && *values[l].first < 0)
                    qDebug() << "Stacked chart with negative values";
                  qreal const y(
                    YofV(*values[l].first + tot,
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
                      QColor fill(values[l].second);
                      fill.setAlpha(100);
                      painter.setBrush(fill);
                      painter.setRenderHint(QPainter::Antialiasing, false);
                      painter.drawConvexPolygon(points, 4);
                      painter.setPen(values[l].second);
                      painter.setRenderHint(QPainter::Antialiasing);
                      painter.drawLine(*last[l], cur);
                    }
                    last[l] = cur;
                    lastOffs[l] = prevY;
                    prevY = y;
                    tot += *values[l].first;
                  } else {
                    last[l] = std::nullopt;
                  }
                } else {
                  last[l] = std::nullopt;
                }
              }
              lastX = x;
        });
      }
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
  std::map<QString, PerFunctionResults> funcs;

  /* First, collect the Function pointer and columns vectors (ie fill
   * the above funcs map) */
  editWidget->iterFields([this,numAxes,&funcs,&axes](
    std::string const &site, std::string const &program,
    std::string const &function, conf::DashboardWidgetChart::Column const &field) {

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
    QString const funcFq(
      QString::fromStdString(site) + ":" +
      QString::fromStdString(program) + "/" +
      QString::fromStdString(function));
    auto it(funcs.find(funcFq));
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

      // Redisplay on new arrivals:
      std::shared_ptr<PastData> past(func->getPast());
      if (past) {
        connect(past.get(), &PastData::tupleReceived,
                this, [this]() {
          update();
        });
      }

      auto emplaced = funcs.emplace(funcFq, func);
      it = emplaced.first;

      /* Also ask for this function's tail: */
      std::shared_ptr<TailModel const> tailModel(func->getTail());
      if (! tailModel) {
        if (verbose)
          qDebug() << "TimeChart: Requesting tail";
        tailModel = func->getOrCreateTail();
        if (tailModel) {
          connect(tailModel.get(), &TailModel::rowsInserted,
                  this, [this,tailModel]() {
            // Signal the new front time
            double const t(tailModel->maxEventTime());
            if (! std::isnan(t)) emit newTailTime(t);
            // Redraw the chart
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

    // Add this field in the request and remember its location:
    PerFunctionResults &res = it->second;
    switch (field.representation) {
      case conf::DashboardWidgetChart::Column::Unused:
        break;  // Well tried!
      case conf::DashboardWidgetChart::Column::Independent:
        axes[field.axisNum].independent.emplace_back(
          &res, field.name, res.columns.size(), field.color);
        break;
      case conf::DashboardWidgetChart::Column::Stacked:
        axes[field.axisNum].stacked.emplace_back(
          &res, field.name, res.columns.size(), field.color);
        break;
      case conf::DashboardWidgetChart::Column::StackCentered:
        axes[field.axisNum].stackCentered.emplace_back(
          &res, field.name, res.columns.size(), field.color);
        break;
    }

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
      // Get the min/max over the whole time range:
      for (std::pair<double, std::vector<std::optional<double>>> const &tuple :
             line.res->tuples) {
        std::optional<double> const &v(tuple.second[line.columnIndex]);
        if (v) updateExtremums(*v, *v);
      }
    }

    /* Set extremums for stacked lines: */
    for (auto &it : funcs) {
      PerFunctionResults &res = it.second;
      Axis::iterTime(res, axis.stacked, [updateExtremums,&axis](
        double,
        std::vector<std::pair<std::optional<qreal>, QColor const &>> values) {
          qreal totHeight(0.);
          for (auto const &value : values) {
            if (value.first) totHeight += *value.first;
          }
          updateExtremums(0., totHeight);
      });
    }

    /* Set extremums for stack-centered lines: */
    for (auto &it : funcs) {
      PerFunctionResults &res = it.second;
      Axis::iterTime(res, axis.stackCentered, [updateExtremums,&axis](
        double,
        std::vector<std::pair<std::optional<qreal>, QColor const &>> values) {
          qreal totHeight(0.);
          for (auto const &value : values) {
            if (value.first) totHeight += *value.first;
          }
          updateExtremums(-totHeight/2, totHeight/2);
      });
    }
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
  PerFunctionResults const &res,
  std::vector<Line> const &lines,
  std::function<void(
    double, std::vector<std::pair<std::optional<qreal>, QColor const &>>)> cb)
{
  if (lines.size() == 0) return;

  size_t columns[res.columns.size()]; // at most
  size_t lineIndex[res.columns.size()];
  size_t numValues(0);
  for (size_t i = 0; i < lines.size(); i++) {
    if (lines[i].res != &res) continue;
    lineIndex[numValues] = i;
    columns[numValues] = lines[i].columnIndex;
    numValues++;
  }
  if (0 == numValues) return;

  for (std::pair<double, std::vector<std::optional<double>>> const &tuple :
         res.tuples) {
    // Build the values
    std::vector<std::pair<std::optional<qreal>, QColor const &>> values;
    values.reserve(numValues);
    for (size_t c = 0; c < numValues; c++) {
      values.emplace_back(
        tuple.second[columns[c]],
        lines[lineIndex[c]].color);
    }

    cb(tuple.first, values);
  }
}
