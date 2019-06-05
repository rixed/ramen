#ifndef CODEINFOPANEL_H_190605
#define CODEINFOPANEL_H_190605
#include <memory>
#include <QLabel>
#include "AtomicWidget.h"
#include "confValue.h"

class CodeInfoPanel : public QLabel, public AtomicWidget
{
  Q_OBJECT

  std::shared_ptr<conf::Value const> value;

public:
  CodeInfoPanel(conf::Key const &key, QWidget *parent = nullptr) :
    QLabel(parent), AtomicWidget(key) {}

  std::shared_ptr<conf::Value const> getValue() const { return value; }

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
