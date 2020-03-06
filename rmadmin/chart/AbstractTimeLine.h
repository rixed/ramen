#ifndef ABSTRACTTIMELINE_H_191204
#define ABSTRACTTIMELINE_H_191204
/* An AbstractTimeLine is a QWidget that can be scrolled to the left and right
 * up to given limits, or zoomed in and out, and that allows the user to
 * select a time (or a time range).
 *
 * This object displays nothing and is supposed to be overloaded with an
 * actual widget displaying some content, such as a time axis with tick marks
 * or a heat map etc.
 *
 * Not to be confused with QTimeLine, which is not a widget but an object to
 * control animations. */
#include <optional>
#include <QWidget>
#include <QPair>

struct TimeRange;

class AbstractTimeLine : public QWidget
{
  /* FIXME: Make this work with relative times.
   * 1. Use a TimeRange instead of begin/end of time as qreal
   * 2. In paintEvent, update the endOfTime/viewPort if timeRange.relative.
   */

  Q_OBJECT
  Q_PROPERTY(qreal beginOfTime
             READ beginOfTime
             WRITE setBeginOfTime
             NOTIFY beginOfTimeChanged)
  Q_PROPERTY(qreal endOfTime
             READ endOfTime
             WRITE setEndOfTime
             NOTIFY endOfTimeChanged)
  Q_PROPERTY(QPair<qreal, qreal> viewPort
             READ viewPort
             WRITE setViewPort
             NOTIFY viewPortChanged)
  Q_PROPERTY(qreal currentTime
             READ currentTime
             WRITE setCurrentTime
             NOTIFY currentTimeChanged
             USER true)
  Q_PROPERTY(QPair<qreal, qreal> selection
             MEMBER m_selection
             NOTIFY selectionChanged)
  Q_PROPERTY(bool withCursor
             MEMBER m_withCursor)
  Q_PROPERTY(bool doScroll
             READ doScroll)

protected:
  void mouseMoveEvent(QMouseEvent *) override;
  void mousePressEvent(QMouseEvent *) override;
  void mouseReleaseEvent(QMouseEvent *) override;
  bool event(QEvent *) override;
  void keyPressEvent(QKeyEvent *) override;
  void enterEvent(QEvent *) override { hovered = true; }
  void leaveEvent(QEvent *) override { hovered = false; }
  void paintEvent(QPaintEvent *) override;

  int toPixel(qreal) const;
  qreal toTime(int) const;

  qreal m_beginOfTime;
  qreal m_endOfTime;
  QPair<qreal, qreal> m_viewPort;
  qreal m_currentTime;
  QPair<qreal, qreal> m_selection;
  bool m_withCursor;
  bool m_doScroll;
  bool hovered;

  QVector<QPair<qreal, qreal>> highlights;

public:
  AbstractTimeLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = false,
    QWidget *parent = nullptr);

  QSize sizeHint() const override { return QSize(250, 20); }

  qreal beginOfTime() const { return m_beginOfTime; }
  qreal endOfTime() const { return m_endOfTime; }
  qreal currentTime() const { return m_currentTime; }
  void setBeginOfTime(qreal);
  void setEndOfTime(qreal);

  qreal viewPortWidth() const { return m_viewPort.second - m_viewPort.first; }

  static QPair<qreal, qreal> noSelection;

  bool doScroll() const { return m_doScroll; }

  void highlightRange(QPair<qreal, qreal> const);
  void resetHighlights();

public slots:
  void setCurrentTime(qreal);

  /* The new viewport will be clipped to the begin and end of times: */
  void setViewPort(QPair<qreal, qreal> const &);
  QPair<qreal, qreal> viewPort() const { return m_viewPort; }

  /* Zoom in or out of the current time. Note that zoom=1 means to have
   * the viewport = begin to end of time, and is therefore the minimum
   * zoom possible. */
  void setZoom(qreal zoom, qreal centerTime);

  /* Move the viewport left or right. */
  void moveViewPort(qreal ratio);

  void setSelection(QPair<qreal, qreal> const &);
  void clearSelection();

  /* This matches TimeRangeEditor signals and allow to control both the
   * beginOfTime and endOfTime, so that the TimeRangeEditor controls the
   * large picture while user is still able to zoom the viewport at will. */
  void setTimeRange(TimeRange const &);

signals:
  void beginOfTimeChanged(qreal);
  void endOfTimeChanged(qreal);
  void viewPortChanged(QPair<qreal, qreal>);
  void currentTimeChanged(qreal);
  void selectionChanged(QPair<qreal, qreal>);

private:
  std::optional<int> scrollStart;
  QPair<qreal, qreal> viewPortStartScroll;
  std::optional<qreal> candidateSelectionStart;
};

#endif
