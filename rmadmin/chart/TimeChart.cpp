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
#include <QSizePolicy>
#include <QStaticText>
#include "chart/Ticks.h"
#include "chart/TimeChartEditWidget.h"
#include "colorOfString.h"
#include "EventTime.h"
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
static int const minorTickLabelHeight(10);
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

  setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
}

std::optional<int> TimeChart::anyAxis(bool left) const
{
  for (int i = 0; i < editWidget->axisCount(); i++) {
    std::optional<conf::DashWidgetChart::Axis> axis = editWidget->axis(i);
    assert(axis);
    if (axis->left == left) return i;
  }

  return std::nullopt;
}

void TimeChart::redrawAxis(int axisNum)
{
  if (verbose)
    qDebug() << "TimeChart::redrawAxis" << axisNum;

  std::optional<conf::DashWidgetChart::Axis const> axis =
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
  std::optional<conf::DashWidgetChart::Axis const> const &conf)
{
  if (conf) {
    switch (conf->scale) {
      case conf::DashWidgetChart::Axis::Linear:
        break;  // default
      case conf::DashWidgetChart::Axis::Logarithmic:
        return std::make_pair(true, 10);
      /* TODO: Later when units have their own scales we could
       * choose another base (such as 2 or 8 for data volumes,
       * 60 for durations...) to pick the ticks */
    }
  }
  return std::make_pair(false, 10);
}

void TimeChart::paintGrid(Axis const &axis)
{
  QPainter painter(this);
  painter.setRenderHint(QPainter::Antialiasing);
  painter.setRenderHint(QPainter::TextAntialiasing);
  QColor const bgColor(palette().color(QWidget::backgroundRole()));
  QColor const gridColor(bgColor.lighter(120));
  QColor const errColor(200, 30, 30);

  if (axis.min >= axis.max) {
    bool const hasEventTime { axis.hasEventTime() };

    QPen pen(hasEventTime ? gridColor : errColor, 1);
    painter.setPen(pen);
    QFont font(painter.font());
    font.setPixelSize(tickLabelHeight);
    painter.setFont(font);
    painter.drawText(rect(), Qt::AlignCenter,
      hasEventTime ? "no data" : "no event-time information");
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
  minorFont.setPixelSize(minorTickLabelHeight);

  std::pair<bool, int> log_base = get_log_base(axis.conf);
  Ticks ticks(axis.min, axis.max, log_base.first, log_base.second);

  int const x2(width() - tickLabelWidth);
  int lastY(-99);
  for (struct Tick const &tick : ticks.ticks) {
    painter.setFont(tick.major ? majorFont : minorFont);
    int const fontHeight(tick.major ? tickLabelHeight : minorTickLabelHeight);
    qreal const y(
      YofV(tick.pos, axis.min, axis.max, log_base.first, log_base.second));
    if (!std::isnan(y)) {
      if (tick.major || std::abs(lastY - y) > fontHeight*2) {
        if (side == Left) {
          painter.drawText(
            QRect(0, y - fontHeight/2, tickLabelWidth, 2*tickLabelHeight),
            Qt::AlignLeft, tick.label);
        } else {
          assert(side == Right);
          painter.drawText(
            QRect(x2, y - fontHeight/2, tickLabelWidth, 2*tickLabelHeight),
            Qt::AlignRight, tick.label);
        }
      }
      lastY = y;
    }
  }
}

void TimeChart::paintAxis(Axis const &axis)
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

    /* Now draw the line, actually one per factor combination. */
    FactorValues const &factorValues(
      line.res->factorValues[line.factorValues]);
    for (std::pair<QString, std::vector<size_t>> const &p : factorValues.labels) {
      QString const &label(p.first);
      std::vector<size_t> const &tupleIndices(p.second);

      /* We use the color of the line if there is only one possible value
       * for the functors, or the color associated with the factor labels
       * otherwise. */
      QColor const color(
        factorValues.labels.size() <= 1 ?
          line.color : colorOfString(label));
      painter.setPen(color);
      std::optional<QPointF> last; // the last point
      bool lastDrawn(false);  // was the last point drawn somehow?
      for (size_t i = 0; i < tupleIndices.size(); i++) {
        CategorizedTuple const &tuple(line.res->tuples[tupleIndices[i]]);
        std::optional<double> const v(tuple.values[line.columnIndex]);
        if (v) {
          qreal const y(
            YofV(*v, axis.min, axis.max, log_base.first, log_base.second));
          if (! std::isnan(y)) {
            QPointF cur(toPixel(tuple.time), y);
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
  }

  /* Draw stacked lines.
   * That's a bit more involved as each stacked "line" can use factors,
   * and then some factor values may be missing for some time steps. */
  std::function<void(std::vector<Line> const &, bool)> drawStacked =
    [this, &axis, log_base](std::vector<Line> const &lines, bool center)
    {
      QPainter painter(this);
      qreal const zeroY(
        YofV(0, axis.min, axis.max, log_base.first, log_base.second));

      size_t numValues(0); // one per line per factor values
      for (Line const &line : lines)
        numValues += line.res->factorValues[line.factorValues].labels.size();

      std::optional<QPointF> last[numValues];
      qreal lastOffs[numValues];
      for (size_t l = 0; l < numValues; l++)
        lastOffs[l] = zeroY;
      qreal lastX(0);

      // Now loop over all factor values of all lines
      Axis::iterTime(lines,
        [this, &axis, &painter, log_base, numValues,
         &last, &lastOffs, &lastX, center](
          double time,
          std::vector<std::pair<std::optional<qreal>, QColor>> values)
      {
        assert(values.size() == numValues);
        if (false && verbose) {
          std::vector<QString> dbgValues;
          dbgValues.reserve(numValues);
          for (auto &p : values)
            dbgValues.push_back(
              p.first ? QString::number(*p.first) : QString("null"));
          qDebug() << qSetRealNumberPrecision(13) << "t=" << time
                   << "values=" << dbgValues;
        }

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
            if (!std::isnan(y)) {
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
  for (int c = 0; c < outType->vtyp->numColumns(); c++) {
    QString const columnName(outType->vtyp->columnName(c));
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
  editWidget->iterFields([this, numAxes, &funcs, &axes](
    std::string const &site, std::string const &program,
    std::string const &function, conf::DashWidgetChart::Column const &field) {

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

    int const factorValues = res.addFactors(field.factors);
    switch (field.representation) {
      case conf::DashWidgetChart::Column::Unused:
        break;  // Well tried!
      case conf::DashWidgetChart::Column::Independent:
        axes[field.axisNum].independent.emplace_back(
          &res, field.name, res.columns.size(), factorValues, field.color);
        break;
      case conf::DashWidgetChart::Column::Stacked:
        axes[field.axisNum].stacked.emplace_back(
          &res, field.name, res.columns.size(), factorValues, field.color);
        break;
      case conf::DashWidgetChart::Column::StackCentered:
        axes[field.axisNum].stackCentered.emplace_back(
          &res, field.name, res.columns.size(), factorValues, field.color);
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
    if (!eventTime || !eventTime->isValid()) {
      qWarning() << "Cannot chart data without an event time";
      res.noEventTime = true;
      continue;
    }

    if (verbose)
      qDebug() << qSetRealNumberPrecision(13)
               << "TimeChart: collecting tuples for" << res.columns.size()
               << "columns of" << res.func->fqName
               << "between" << m_viewPort.first << "and" << m_viewPort.second;

    res.func->iterValues(m_viewPort.first, m_viewPort.second, true, res.columns,
      [&res](double time, std::vector<RamenValue const *> const values) {
      /* Store this tuple (as doubles) */
      size_t const tupleIdx(res.tuples.size());
      res.tuples.emplace_back(time, values);

      /* Also update the possible values for every defined combination of
       * factors: */
      std::string const noStr;
      for (FactorValues &f : res.factorValues) {
        QString label;
        for (std::pair<std::string, int> const &idx : f.indices) {
          QString const value(values[idx.second]->toQString(noStr));
          label += QString::fromStdString(idx.first) + "=" + value +" ";
        }
        auto emplaced =
          f.labels.emplace(std::piecewise_construct,
                           std::forward_as_tuple(label),
                           std::forward_as_tuple());
        bool const isNew(emplaced.second);
        std::vector<size_t> &tupleIndices(emplaced.first->second);
        if (isNew) tupleIndices.reserve(100);
        tupleIndices.push_back(tupleIdx);

      }
    });

    if (verbose) {
      qDebug() << "TimeChart: got" << res.tuples.size() << "tuples";
//      for (FactorValues &f : res.factorValues) {
//        for (std::pair<QString, std::vector<size_t>> const &p : f.labels) {
//          qDebug() << "  " << p.first << ":" << p.second.size();
//        }
//      }
    }
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
      for (CategorizedTuple const &tuple : line.res->tuples) {
        std::optional<double> const &v(tuple.values[line.columnIndex]);
        if (v) updateExtremums(*v, *v);
      }
    }

    /* Set extremums for stacked lines. */
    Axis::iterTime(axis.stacked, [updateExtremums](
      double,
      std::vector<std::pair<std::optional<qreal>, QColor>> values) {
        qreal totHeight(0.);
        for (auto const &value : values) {
          if (value.first) totHeight += *value.first;
        }
        updateExtremums(0., totHeight);
    });

    /* Set extremums for stack-centered lines. */
    Axis::iterTime(axis.stackCentered, [updateExtremums](
      double,
      std::vector<std::pair<std::optional<qreal>, QColor>> values) {
        qreal totHeight(0.);
        for (auto const &value : values) {
          if (value.first) totHeight += *value.first;
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
    paintGrid(axes[*focusedGridAxis]);
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

    paintAxis(axes[i]);
  }

  // Finally, the focused one:
  if (focusedGridAxis)
    paintAxis(axes[*focusedGridAxis]);
}

size_t TimeChart::PerFunctionResults::addFactors(
  std::vector<std::string> const &factors)
{
  std::vector<std::pair<std::string, int>> factorIndices;
  factorIndices.reserve(2);

  bool canHaveIt(true);
  for (std::string const &factor : factors) {
    int const factorNum(getFieldNum(func, factor));
    if (factorNum < 0) continue;
    // If this column was already asked, reuse it:
    for (int idx = 0; idx < (int)columns.size(); idx++) {
      if (columns[idx] == factorNum) {
        factorIndices.emplace_back(factor, idx );
        goto found;
      }
    }
    // otherwise, add it to the list of requested columns:
    factorIndices.emplace_back(factor, columns.size());
    columns.push_back(factorNum);
    canHaveIt = false;  // no need to look if we already have this combination
found:;
  }

  // Maybe we have this exact combination of factors already?
  if (canHaveIt) {
    for (size_t i = 0; i < factorValues.size(); i++) {
      FactorValues const &f(factorValues[i]);
      if (f.indices.size() != factorIndices.size()) continue;
      size_t j(0);
      for (; j < factorIndices.size(); j++) {
        // No need to compare the names
        if (f.indices[j].second != factorIndices[j].second) break;
      }
      if (j == factorIndices.size()) return i;
    }
  }

  factorValues.emplace_back(factorIndices);

  return factorValues.size() - 1;
}

/* For each time step, call the provided function with a vector of one value
 * per factor value per line. */
void TimeChart::Axis::iterTime(
  std::vector<Line> const &lines,
  std::function<void(
    double, std::vector<std::pair<std::optional<qreal>, QColor>>)> cb)
{
  /* For every time step, we will have as many values as we have possible
   * factor values per lines: */
  size_t numValues(0);
  for (Line const &line : lines)
    numValues += line.res->factorValues[line.factorValues].labels.size();

  if (0 == numValues) return;

  std::vector<std::pair<std::optional<qreal>, QColor>> values(
    numValues, std::make_pair(std::nullopt, QColor()));

  size_t numTuples[numValues];

  size_t valIdx(0);
  for (Line const &line : lines) {
    FactorValues const &factorValues(line.res->factorValues[line.factorValues]);
    bool const noFactor(factorValues.labels.size() <= 1);
    for (std::pair<QString, std::vector<size_t>> const &p :
           factorValues.labels) {
      QString const &label(p.first);
      std::vector<size_t> const &tupleIndices(p.second);

      values[valIdx].second = noFactor ? line.color : colorOfString(label);
      numTuples[valIdx] = tupleIndices.size();

      valIdx++;
    }
  }

  /* Iterate over time steps: */
  std::vector<size_t> nextTupleIdx(numValues, 0);
  std::vector<bool> done(numValues, false);
  unsigned numDone(0);

  while (numDone < numValues) {
    size_t valIdx(0);
    double minTime(std::numeric_limits<double>::max());

    for (Line const &line : lines) {
      FactorValues const &factorValues(line.res->factorValues[line.factorValues]);

      for (std::pair<QString, std::vector<size_t>> const &p :
             factorValues.labels) {
        std::vector<size_t> const &tupleIndices(p.second);

        if (!done[valIdx]) {
          size_t const tupleIdx(tupleIndices[nextTupleIdx[valIdx]]);
          CategorizedTuple const &tuple(line.res->tuples[tupleIdx]);

          minTime = std::min(minTime, tuple.time);
        }

        valIdx++;
      }
    }

    /* Now we know the minTime, aggregate all values that we want in
     * this time step. Empty the values that are too far in the future. */
    static double const minDT(.0001);  // Helps with epsilons
    valIdx = 0;
    for (Line const &line : lines) {
      FactorValues const &factorValues(line.res->factorValues[line.factorValues]);

      for (std::pair<QString, std::vector<size_t>> const &p :
             factorValues.labels) {
        std::vector<size_t> const &tupleIndices(p.second);

        values[valIdx].first = 0.;

        if (!done[valIdx]) {
          /* While nextTupleIdx < limit and time is in range, sum the value.
           * Then iterTime should sum the values, and we should then also use
           * iterTime to draw independent lines, and the DT used for the pivot
           * should be configurable. */
          while (true) {
            size_t const tupleIdx(tupleIndices[nextTupleIdx[valIdx]]);
            CategorizedTuple const &tuple(line.res->tuples[tupleIdx]);
            if (tuple.time > minTime + minDT) break;
            *values[valIdx].first += tuple.values[line.columnIndex].value_or(0.);
            if (++nextTupleIdx[valIdx] >= numTuples[valIdx]) {
              done[valIdx] = true;
              numDone++;
              break;
            }
          }
        }

        valIdx++;
      }
    }

    cb(minTime, values);
  }
}

bool TimeChart::Axis::hasEventTime() const
{
  for (Line const &l : stacked) if (l.res->noEventTime) return false;
  for (Line const &l : stackCentered) if (l.res->noEventTime) return false;
  for (Line const &l : independent) if (l.res->noEventTime) return false;

  return true;
}
