#ifndef KSHORTLABEL_H_190729
#define KSHORTLABEL_H_190729
/* Like a KLabel, but the content will never grow beyond the container size.
 * Based on Qt example at
 * https://doc.qt.io/qt-5/qtwidgets-widgets-elidedlabel-example.html */
#include <QFrame>
#include "AtomicWidget.h"

class KShortLabel : public AtomicWidget
{
  Q_OBJECT

  QFrame *frame;
  QString text;

  int leftMargin, topMargin, rightMargin, bottomMargin;

public:
  KShortLabel(conf::Key const &, QWidget *parent = nullptr);

  void setContentsMargins(int, int, int, int);

  void setEnabled(bool) {} // not editable

protected:
  void paintEvent(QPaintEvent *event);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
