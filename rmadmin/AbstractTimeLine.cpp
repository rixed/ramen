#include <QDebug>
#include <QEnterEvent>
#include <QMouseEvent>
#include <QPainter>
#include <QPaintEvent>
#include <QPoint>
#include <QSizePolicy>
#include <QWheelEvent>
#include "misc.h"
#include "AbstractTimeLine.h"

static bool const verbose = true;

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

void AbstractTimeLine::wheelEvent(QWheelEvent *event)
{
  if (! m_doScroll) {
ignore:
    QWidget::wheelEvent(event); // will ignore() the event
    return;
  }

  QPoint p(event->pixelDelta());
  if (p.isNull()) {
    p = event->angleDelta() / 8;
  }

  if (! p.isNull()) {
    qreal zoom = p.y() >= 0 ? 0.97 : 1.03;

    qreal const leftWidth = zoom * (m_currentTime - m_viewPort.first);
    qreal const rightWidth = zoom * (m_viewPort.second - m_currentTime);

    auto newViewPort = QPair<qreal, qreal>(
      std::max(m_currentTime - leftWidth, m_beginOfTime),
      std::min(m_currentTime + rightWidth, m_endOfTime));
    if (newViewPort == m_viewPort) goto ignore;

    setViewPort(newViewPort);
    emit viewPortChanged(m_viewPort);
  }

  /* Prevent propagation to any QScrollArea: */
  event->accept();
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
    m_beginOfTime = t;
    update();
  }
}

void AbstractTimeLine::setEndOfTime(qreal t)
{
  if (m_endOfTime != t) {
    m_endOfTime = t;
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
  /* Should not happend but better safe than sorry: */
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

  qDebug() << "viewPort =" << stringOfDate(m_viewPort.first)
           << "..." << stringOfDate(m_viewPort.second);

  update();
}

void AbstractTimeLine::setZoom(qreal z)
{
  if (z <= 0) {
    qCritical() << "Invalid zoom" << z;
    return;
  }

  qreal const dt(m_endOfTime - m_beginOfTime);
  qreal const vp_half_width = 0.5 * dt / z;

  setViewPort(QPair<qreal, qreal>(
    std::max(m_currentTime - vp_half_width, m_beginOfTime),
    std::min(m_currentTime + vp_half_width, m_endOfTime)));
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
