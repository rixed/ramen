#ifndef TIMECHARTEDITWIDGET_H_200306
#define TIMECHARTEDITWIDGET_H_200306
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include "AtomicWidget.h"
#include "confValue.h"

class QPushButton;
class TimeChartFunctionsEditor;
class TimeChartOptionsEditor;

class TimeChartEditWidget : public AtomicWidget
{
  Q_OBJECT

  TimeChartOptionsEditor *optionsEditor;
  TimeChartFunctionsEditor *functionsEditor;
  QPushButton *cancelButton;

public:
  TimeChartEditWidget(
    QPushButton *submitButton,
    QPushButton *cancelButton,
    QWidget *parent = nullptr);

  void setEnabled(bool) override;

  bool setValue(std::string const &, std::shared_ptr<conf::Value const>) override;

  std::shared_ptr<conf::Value const> getValue() const override;

  int axisCountOnSide(bool left) const;

  // Returns the axis number, that can then later given to axis()
  std::optional<int> firstAxisOnSide(bool left) const;

  int axisCount() const;

  std::optional <conf::DashWidgetChart::Axis const> axis(int) const;

  void iterFields(std::function<void(
    std::string const &site, std::string const &program,
    std::string const &function, conf::DashWidgetChart::Column const &)>) const;

  void closeEvent(QCloseEvent *) override;

signals:
  void axisChanged(int);
  void fieldChanged(std::string const &site, std::string const &program,
                    std::string const &function, std::string const &name);
  // TODO: valueChanged, inputChanged?
};

#endif
