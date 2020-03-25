#ifndef TIMECHARTFUNCTIONSEDITOR_H_200306
#define TIMECHARTFUNCTIONSEDITOR_H_200306
#include <memory>
#include <list>
#include <QWidget>
#include "confValue.h"

class Function;
class FunctionSelector;
class QToolBox;
class TimeChartFunctionEditor;

class TimeChartFunctionsEditor : public QWidget
{
  Q_OBJECT

  void allFieldsChanged(int);
  TimeChartFunctionEditor *addFunctionByName(
    std::string const &site, std::string const &program,
    std::string const &function, bool customizable);
  void addOrFocus(
    std::string const &site, std::string const &program,
    std::string const &function, bool customizable);

public:
  QToolBox *functions;
  FunctionSelector *functionSelector;

  TimeChartFunctionsEditor(QWidget *parent = nullptr);
  bool setValue(std::shared_ptr<conf::DashWidgetChart const> v);
  void setEnabled(bool);

protected slots:
  void addCurrentFunction();
  void addCustomizedFunction(std::string const &site, std::string const &program,
                             std::string const &function);

signals:
  void fieldChanged(std::string const &site, std::string const &program,
                    std::string const &function, std::string const &name);
};

#endif
