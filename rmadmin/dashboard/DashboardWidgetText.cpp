#include <QLabel>
#include <QVBoxLayout>
#include "confValue.h"

#include "dashboard/DashboardWidgetText.h"

DashboardWidgetText::DashboardWidgetText(
  std::string const &key,
  QWidget *parent)
  : DashboardWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;
  (void)key;
  text = new QLabel("TODO: an actual dasboard title editor");
  setLayout(layout);
}
