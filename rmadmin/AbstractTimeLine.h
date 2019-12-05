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

class AbstractTimeLine : public QWidget
{
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
  void wheelEvent(QWheelEvent *) override;
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

public slots:
  void setCurrentTime(qreal);

  /* The new viewport will be clipped to the begin and end of times: */
  void setViewPort(QPair<qreal, qreal> const &);
  QPair<qreal, qreal> viewPort() const { return m_viewPort; }

  /* Zoom in or out of the current time. Note that zoom=1 means to have
   * the viewport = begin to end of time, and is therefore the minimum
   * zoom possible. */
  void setZoom(qreal);

  void setSelection(QPair<qreal, qreal> const &);
  void clearSelection();

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
