#include <QVBoxLayout>
#include "confValue.h"
#include "dashboard/DashboardTextEditor.h"

#include "dashboard/DashboardWidgetText.h"

DashboardWidgetText::DashboardWidgetText(
  std::string const &,
  QWidget *parent)
  : DashboardWidget(parent)
{
  editor = new DashboardTextEditor;

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(editor);
  setLayout(layout);
}
