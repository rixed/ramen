#ifndef DASHBOARDTEXTEDITOR_H_200309
#define DASHBOARDTEXTEDITOR_H_200309
#include <memory>
#include "AtomicWidget.h"

class QLineEdit;
class QWidget;

namespace conf {
  class Value;
};

class DashboardTextEditor : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *text;

public:
  DashboardTextEditor(QWidget *parent = nullptr);
  void setEnabled(bool);
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
  std::shared_ptr<conf::Value const> getValue() const;

};

#endif
