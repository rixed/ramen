#ifndef KFLOATEDITOR_H_190727
#define KFLOATEDITOR_H_190727
#include <QLineEdit>
#include "confValue.h"
#include "AtomicWidget.h"

struct KFloatEditor : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *lineEdit;

  void extraConnections(KValue *);

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
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
