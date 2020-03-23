#ifndef DASHBOARDWIDGETTEXT_H_200304
#define DASHBOARDWIDGETTEXT_H_200304
#include <string>
#include "AtomicWidget.h"

class DashboardWidgetForm;
class QTextEdit;

class DashboardWidgetText : public AtomicWidget
{
  Q_OBJECT

  QTextEdit *text;

public:
  DashboardWidgetText(
    DashboardWidgetForm *,
    QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
