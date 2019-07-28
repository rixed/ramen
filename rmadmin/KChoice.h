#ifndef KCHOICE_H_190516
#define KCHOICE_H_190516
#include <vector>
#include <utility>
#include <memory>
#include <QWidget>
#include "AtomicWidget.h"

class QRadioButton;

class KChoice : public AtomicWidget
{
  Q_OBJECT

  QWidget *widget;
  std::vector<std::pair<QRadioButton *, std::shared_ptr<conf::Value const>>> choices;

public:
  KChoice(conf::Key const &key, std::vector<std::pair<QString const, std::shared_ptr<conf::Value const>>> labels, QWidget *parent = nullptr);

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const> v);
  void lockValue(conf::Key const &k, QString const &uid)
  {
    AtomicWidget::lockValue(k, uid);
  }
  void unlockValue(conf::Key const &k)
  {
    AtomicWidget::unlockValue(k);
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
