#include <QHBoxLayout>
#include <QLabel>

#include "dashboard/DashboardWidgetEditor.h"

DashboardWidgetEditor::DashboardWidgetEditor(QWidget *parent)
  : QWidget(parent)
{
  QLabel *l = new QLabel("DashboardWidgetEditor: delete me!");
  QHBoxLayout *layout = new QHBoxLayout;
  layout->addWidget(l);
  setLayout(layout);
}
