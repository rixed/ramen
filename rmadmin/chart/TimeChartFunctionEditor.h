#ifndef TIMECHARTFUNCTIONEDITOR_H_200306
#define TIMECHARTFUNCTIONEDITOR_H_200306
#include <memory>
#include <string>
#include <QSize>
#include <QWidget>
#include "confValue.h"  // for the inner DashboardWidgetChart::Source

class KTextEdit;
class QCheckBox;
class QLineEdit;
class QTableView;
class TimeChartFunctionFieldsModel;

class TimeChartFunctionEditor : public QWidget
{
  Q_OBJECT

public:
  QCheckBox *visible;   // To disable the whole source temporarily
  KTextEdit *inlineFuncEdit;  // if this is an inline function
  QTableView *fields;

  TimeChartFunctionFieldsModel *model;

  TimeChartFunctionEditor(
    std::string const &site,
    std::string const &program,
    std::string const &function,
    QWidget *parent = nullptr);

public slots:
  void setEnabled(bool);
  bool setValue(conf::DashWidgetChart::Source const &);
  conf::DashWidgetChart::Source getValue() const;

signals:
  void fieldChanged(std::string const &site, std::string const &program,
                    std::string const &function, std::string const &name);
};

#endif
