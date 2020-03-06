#ifndef TIMECHARTFUNCTIONSEDITOR_H_200306
#define TIMECHARTFUNCTIONSEDITOR_H_200306
#include <memory>
#include <list>
#include <QToolBox>
#include <QWidget>
#include "confValue.h"

class TimeChartFunctionEditor;

class TimeChartFunctionsEditor : public QToolBox
{
  Q_OBJECT

  void allFieldsChanged(int);

public:
  TimeChartFunctionsEditor(QWidget *parent = nullptr);
  bool setValue(std::shared_ptr<conf::DashboardWidgetChart const> v);

signals:
  void fieldChanged(std::string const &site, std::string const &program,
                    std::string const &function, std::string const &name);
};

#endif
