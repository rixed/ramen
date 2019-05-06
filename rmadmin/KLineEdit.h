#ifndef KLINEEDIT_H_190505
#define KLINEEDIT_H_190505
#include <QLineEdit>
#include "KWidget.h"
#include "confValue.h"

class KLineEdit : public QLineEdit, public KWidget
{
  Q_OBJECT

public:
  KLineEdit(std::string const key, QWidget *parent = nullptr) :
    QLineEdit(parent),
    KWidget(key)
  {}
  ~KLineEdit() {}

public slots:
  void setEnabled(bool enabled)
  {
    QLineEdit::setEnabled(enabled);
  }

  void setValue(conf::Value const &v)
  {
    QLineEdit::setText(v.toQString());
  }

  void delValue(std::string const &)
  {
    // TODO: replace this widget with s tombstone?
  }
};

#endif
