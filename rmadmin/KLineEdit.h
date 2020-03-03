#ifndef KLINEEDIT_H_190505
#define KLINEEDIT_H_190505
#include <memory>
#include <QLineEdit>
#include "AtomicWidget.h"
#include "confValue.h"

class KLineEdit : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *lineEdit;

public:
  KLineEdit(QWidget *parent = nullptr);

  void setPlaceholderText(QString const s) {
    lineEdit->setPlaceholderText(s);
  }

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
