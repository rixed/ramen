#include <QLabel>
#include "confValue.h"
#include "conf.h"

#include "chart/TimeChartEditWidget.h"

TimeChartEditWidget::TimeChartEditWidget(QWidget *parent)
  : AtomicWidget(parent)
{
  QLabel *placeHolder = new QLabel("here soon an editor");
  relayoutWidget(placeHolder);
}

void TimeChartEditWidget::setEnabled(bool enabled)
{
  (void)enabled;
  // TODO
}

bool TimeChartEditWidget::setValue(
  std::string const &k, std::shared_ptr<conf::Value const> v)
{
  (void)k;
  (void)v;
  // TODO
  return true;
}
