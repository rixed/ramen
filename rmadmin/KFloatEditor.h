#ifndef KFLOATEDITOR_H_190727
#define KFLOATEDITOR_H_190727
#include <QLineEdit>
#include "confValue.h"
#include "AtomicWidget.h"


struct KFloatEditor : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *lineEdit;

public:
  KFloatEditor(QWidget *parent = nullptr,
               double min = -std::numeric_limits<double>::infinity(),
               double max = std::numeric_limits<double>::infinity());

  void setPlaceholderText(QString const s) {
    lineEdit->setPlaceholderText(s);
  }

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
