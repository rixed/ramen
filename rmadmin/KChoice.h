#ifndef KCHOICE_H_190516
#define KCHOICE_H_190516
#include <memory>
#include <utility>
#include <vector>
#include "AtomicWidget.h"

class QRadioButton;
class QWidget;

class KChoice : public AtomicWidget
{
  Q_OBJECT

  std::vector<std::pair<QRadioButton *, std::shared_ptr<conf::Value const>>> choices;

public:
  KChoice(std::vector<std::pair<QString const, std::shared_ptr<conf::Value const>>> labels, QWidget *parent = nullptr);

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const> v);
};

#endif
