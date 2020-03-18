#ifndef TIMECHARTFUNCTIONSEDITOR_H_200306
#define TIMECHARTFUNCTIONSEDITOR_H_200306
#include <memory>
#include <list>
#include <QWidget>
#include "confValue.h"

class FunctionSelector;
class QToolBox;
class TimeChartFunctionEditor;

class TimeChartFunctionsEditor : public QWidget
{
  Q_OBJECT

  void allFieldsChanged(int);

public:
  QToolBox *functions;
  FunctionSelector *functionSelector;

  TimeChartFunctionsEditor(QWidget *parent = nullptr);
  bool setValue(std::shared_ptr<conf::DashboardWidgetChart const> v);
  void setEnabled(bool);

protected slots:
  void addFunction();

signals:
  void fieldChanged(std::string const &site, std::string const &program,
                    std::string const &function, std::string const &name);
};

#endif
