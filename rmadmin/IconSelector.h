#ifndef ICONSELECTOR_H_200316
#define ICONSELECTOR_H_200316
/* A simple widget offering to select one amongst several icons */
#include <optional>
#include <QList>
#include <QPixmap>
#include <QSize>
#include <QWidget>

class IconSelector : public QWidget
{
  Q_OBJECT

  int m_selected;
  Q_PROPERTY(int selected READ selected WRITE setSelected NOTIFY selectionChanged);

  QList<QPixmap> pixmaps;

  QSize minSize;

  static int const iconMargin;

  std::optional<int> iconAtPos(QPoint const &);

  std::optional<int> hovered, clicked;

public:
  /* TODO: select orientation */
  IconSelector(QList<QPixmap>, QWidget *parent = nullptr);

  QSize sizeHint() const override { return minSize; }

  int selected() const { return m_selected; }

  void setSelected(int s) { m_selected = s; }

  static void paint(
    QPainter &,
    QRect const &rect,
    QList<QPixmap> const &,
    std::optional<int> selected = std::nullopt,
    std::optional<int> hovered = std::nullopt);

  void paintEvent(QPaintEvent *) override;

  void mouseMoveEvent(QMouseEvent *) override;
  void mousePressEvent(QMouseEvent *) override;
  void mouseReleaseEvent(QMouseEvent *) override;

signals:
  void selectionChanged(int selected);
};

#endif
