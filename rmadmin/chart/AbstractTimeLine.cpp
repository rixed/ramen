#include <QDebug>
#include <QEnterEvent>
#include <QMouseEvent>
#include <QPainter>
#include <QPaintEvent>
#include <QPinchGesture>
#include <QPoint>
#include <QSizePolicy>
#include <QWheelEvent>
#include "misc.h"
#include "TimeRange.h"

#include "AbstractTimeLine.h"

static bool const verbose(false);

AbstractTimeLine::AbstractTimeLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor, bool doScroll, QWidget *parent)
  : QWidget(parent),
    m_beginOfTime(std::min(beginOftime, endOfTime)),
    m_endOfTime(std::max(beginOftime, endOfTime)),
    m_viewPort(QPair<qreal, qreal>(m_beginOfTime, m_endOfTime)),
    m_currentTime(m_beginOfTime), // first mouse move will set this more accurately
    m_selection(noSelection),
    m_withCursor(withCursor),
    m_doScroll(doScroll),
    hovered(false)
{
  setMouseTracking(true);
  setSizePolicy(QSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum));
  grabGesture(Qt::PinchGesture);
  setFocusPolicy(Qt::StrongFocus);
}

/*
 * Utilities
 */

int AbstractTimeLine::toPixel(qreal t) const
{
  return static_cast<int>(
    ((t - m_viewPort.first) / viewPortWidth()) * width());
}

qreal AbstractTimeLine::toTime(int x) const
{
  return
    (static_cast<qreal>(x) / width()) * viewPortWidth() + m_viewPort.first;
}

/*
 * Event Handling
 */

void AbstractTimeLine::mousePressEvent(QMouseEvent *event)
{
  switch (event->button()) {
    case Qt::LeftButton:
      if (m_doScroll) {
        scrollStart = event->x();
        viewPortStartScroll = m_viewPort;
      } else {
        QWidget::mousePressEvent(event);
      }
      break;

    case Qt::RightButton:
      /* Start a new (candidate) selection: */
      candidateSelectionStart = m_currentTime;

      if (verbose)
        qDebug() << "candidate selection started at"
                 << stringOfDate(m_currentTime);
      break;

    default:
      /* Do not call this unless we really want to ignore the event, as
       * otherwise window-manager might start a window move for instance */
      QWidget::mousePressEvent(event);
      break;
  }
}

void AbstractTimeLine::mouseReleaseEvent(QMouseEvent *event)
{
  /* If there was a selection going on, submit it: */
  if (candidateSelectionStart) {
    if (verbose)
      qDebug() << "candidate selection stopped at"
               << stringOfDate(m_currentTime);
    setSelection(QPair<qreal, qreal>(*candidateSelectionStart, m_currentTime));
    candidateSelectionStart.reset();
    emit selectionChanged(m_selection);
  }

  if (scrollStart) {
    scrollStart.reset();
  }

  QWidget::mouseReleaseEvent(event);
}

void AbstractTimeLine::mouseMoveEvent(QMouseEvent *event)
{
  if (scrollStart) {
    /* Scrolling mode: offset the viewport */
    int const dx = event->x() - *scrollStart;
    qreal const ratio = (qreal)dx / width();
    qreal dt = viewPortWidth() * ratio;
    if (viewPortStartScroll.first - dt < m_beginOfTime) {
      dt = viewPortStartScroll.first - m_beginOfTime;
    } else if (viewPortStartScroll.second - dt > m_endOfTime) {
      dt = viewPortStartScroll.second - m_endOfTime;
    }
    setViewPort(QPair<qreal, qreal>(
      std::max(viewPortStartScroll.first - dt, m_beginOfTime),
      std::min(viewPortStartScroll.second - dt, m_endOfTime)));
    emit viewPortChanged(m_viewPort);
  }

  /* Normal mouse move: update current time.
   * Note: selection might happen at the same time. */
  qreal const ratio = (qreal)event->x() / width();
  qreal const offs = viewPortWidth() * ratio;
  qreal t = m_viewPort.first + offs;
  if (t < m_beginOfTime) t = m_beginOfTime;
  else if (t > m_endOfTime) t = m_endOfTime;
  if (t != m_currentTime) {
    setCurrentTime(t);
    emit currentTimeChanged(t);
  }
  QWidget::mouseMoveEvent(event);
}

bool AbstractTimeLine::event(QEvent *event)
{
  if (event->type() != QEvent::Gesture)
    return QWidget::event(event);

  QGestureEvent *e = static_cast<QGestureEvent *>(event);
  QGesture *pinch_ = e->gesture(Qt::PinchGesture);
  if (! pinch_)
    return QWidget::event(event);

  QPinchGesture *pinch = static_cast<QPinchGesture *>(pinch_);

  switch (pinch->state()) {
    case Qt::GestureUpdated:
      {
        qreal const centerTime = toTime(pinch->centerPoint().x());
        setZoom(pinch->scaleFactor(), centerTime);
      }
      break;
    default:
      break;
  }
  return true;
}

void AbstractTimeLine::keyPressEvent(QKeyEvent *event)
{
  switch (event->key()) {
    case Qt::Key_Plus:
      setZoom(1.1, m_currentTime);
      return;
    case Qt::Key_Minus:
      setZoom(0.9, m_currentTime);
      return;
    case Qt::Key_Left:
      moveViewPort(-0.1);
      return;
    case Qt::Key_Right:
      moveViewPort(0.1);
      return;
  }

  QWidget::keyPressEvent(event);
}

/*
 * Painting
 */

void AbstractTimeLine::paintEvent(QPaintEvent *event)
{
  QWidget::paintEvent(event);

  QPainter painter(this);

  static QColor const highlightColor(255, 255, 255, 125);
  painter.setPen(Qt::NoPen);
  painter.setBrush(highlightColor);
  for (QPair<qreal, qreal> const range : highlights) {
    int const xStart = toPixel(range.first);
    int const xStop = toPixel(range.second);
    painter.drawRect(xStart, 0, xStop - xStart, height());
  }

  if (m_withCursor && m_currentTime > m_beginOfTime) {
    static QColor const cursorColor("orange");
    painter.setPen(cursorColor);
    painter.setBrush(Qt::NoBrush);
    int const x = toPixel(m_currentTime);
    painter.drawLine(x, 0, x, height());
  }
}

/*
 * Properties and slots
 */

void AbstractTimeLine::setBeginOfTime(qreal t)
{
  if (m_beginOfTime != t) {
    if (verbose)
      qDebug() << "AbstractTimeLine: setBeginOfTime"<< t;

    // If we were looking at the end, keep tracking
    bool const trackEnd(m_viewPort.second >= m_endOfTime);

    if (m_endOfTime <= t)
      m_endOfTime += t - m_beginOfTime;

    m_beginOfTime = t;
    if (m_viewPort.first < t) {
      m_viewPort.second =
        std::min<qreal>(m_endOfTime, m_viewPort.second + (t - m_viewPort.first));
      m_viewPort.first = t;
    }

    if (trackEnd) {
      double const dt(m_endOfTime - m_viewPort.second);
      m_viewPort.first += dt;
      m_viewPort.second += dt;
    }

    update();
  }
}

void AbstractTimeLine::setEndOfTime(qreal t)
{
  if (m_endOfTime != t) {
    if (verbose)
      qDebug() << "AbstractTimeLine: setEndOfTime"<< t;

    // If we were looking at the end, keep tracking
    bool const trackEnd(m_viewPort.second >= m_endOfTime);

    if (m_beginOfTime >= t)
      m_beginOfTime -= m_endOfTime - t;

    m_endOfTime = t;
    if (m_viewPort.second > t) {
      m_viewPort.first =
        std::max<qreal>(m_beginOfTime, m_viewPort.first - (m_viewPort.second - t));
      m_viewPort.second = t;
    }

    if (trackEnd) {
      double const dt(m_endOfTime - m_viewPort.second);
      m_viewPort.first += dt;
      m_viewPort.second += dt;
    }

    update();
  }
}

void AbstractTimeLine::setCurrentTime(qreal t)
{
  if (t < m_beginOfTime) t = m_beginOfTime;
  else if (t > m_endOfTime) t = m_endOfTime;

  if (t != m_currentTime) {
    m_currentTime = t;
    update();
  }
}

void AbstractTimeLine::setViewPort(QPair<qreal, qreal> const &vp)
{
  /* Should not happen but better safe than sorry: */
  if (vp.first < m_beginOfTime || vp.first >= m_endOfTime ||
      vp.second <= m_beginOfTime || vp.second > m_endOfTime) {
    qCritical() << "AbstractTimeLine: Invalid viewPort:" << vp;
    return;
  }

  m_viewPort = QPair<qreal, qreal>(
    std::min<qreal>(vp.first, vp.second),
    std::max<qreal>(vp.first, vp.second));

  static qreal const min_viewport_width = 1.;
  if (viewPortWidth() < min_viewport_width) {
    qreal const mid = 0.5 * (m_viewPort.second + m_viewPort.first);
    m_viewPort.first = mid - min_viewport_width/2;
    m_viewPort.second = mid + min_viewport_width/2;
  }

  if (verbose)
    qDebug() << "viewPort =" << stringOfDate(m_viewPort.first)
             << "..." << stringOfDate(m_viewPort.second);

  update();
}

void AbstractTimeLine::setZoom(qreal z, qreal centerTime)
{
  if (z <= 0) {
    qCritical() << "Invalid zoom" << z;
    return;
  }

  if (verbose)
    qDebug() << "AbstractTimeLine: setZoom(" << z << "," << centerTime << ")";

  setViewPort(QPair<qreal, qreal>(
    std::max(centerTime - (centerTime - m_viewPort.first) / z, m_beginOfTime),
    std::min(centerTime + (m_viewPort.second - centerTime) / z, m_endOfTime)));
  emit viewPortChanged(m_viewPort);
}

void AbstractTimeLine::moveViewPort(qreal ratio)
{
  qreal const dt = viewPortWidth() * ratio;
  setViewPort(QPair<qreal, qreal>(
    std::max(m_viewPort.first + dt, m_beginOfTime),
    std::min(m_viewPort.second + dt, m_endOfTime)));
  emit viewPortChanged(m_viewPort);
}

void AbstractTimeLine::setSelection(QPair<qreal, qreal> const &sel)
{
  m_selection = QPair<qreal, qreal>(
    std::min<qreal>(sel.first, sel.second),
    std::max<qreal>(sel.first, sel.second));

  qDebug() << "selection =" << stringOfDate(m_selection.first)
           << "..." << stringOfDate(m_selection.second);
}

QPair<qreal, qreal> AbstractTimeLine::noSelection(0, 0);

void AbstractTimeLine::clearSelection()
{
  m_selection = noSelection;
}

void AbstractTimeLine::setTimeRange(TimeRange const &range)
{
  double since, until;
  range.absRange(&since, &until);
  if (verbose)
    qDebug() << "AbstractTimeLine: setTimeRange(" << since << "," << until << ")";
  setBeginOfTime(since);
  setEndOfTime(until);
}

void AbstractTimeLine::highlightRange(QPair<qreal, qreal> const range)
{
  if (verbose)
    qDebug() << "AbstractTimeLine: highlight range" << range;
  highlights.append(range);
  update();
}

void AbstractTimeLine::resetHighlights()
{
  if (verbose)
    qDebug() << "AbstractTimeLine: reset highlights";
  highlights.clear();
  update();
}
