#include <QPushButton>
#include <QTextDocument>
#include <QTextEdit>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include "confValue.h"
#include "dashboard/DashboardWidgetForm.h"

#include "dashboard/DashboardWidgetText.h"

DashboardWidgetText::DashboardWidgetText(
  DashboardWidgetForm *widgetForm,
  QWidget *parent)
  : AtomicWidget(parent)
{
  text = new QTextEdit;
  text->setPlaceholderText(tr("Enter a text here"));

  QHBoxLayout *buttonsLayout = new QHBoxLayout;
  buttonsLayout->addStretch();
  buttonsLayout->addWidget(widgetForm->cancelButton);
  buttonsLayout->addWidget(widgetForm->submitButton);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(text);
  layout->addLayout(buttonsLayout);
  QWidget *widget = new QWidget(this);
  widget->setLayout(layout);

  widgetForm->cancelButton->setVisible(false);
  widgetForm->submitButton->setVisible(false);
  /* Open/close the editor when the AtomicForm is enabled/disabled: */
  connect(widgetForm, &DashboardWidgetForm::changeEnabled,
          this, [widgetForm](bool enabled) {
    widgetForm->cancelButton->setVisible(enabled);
    widgetForm->submitButton->setVisible(enabled);
  });

  relayoutWidget(text);
}

void DashboardWidgetText::setEnabled(bool enabled)
{
  text->setEnabled(enabled);
}

bool DashboardWidgetText::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v_)
{
  std::shared_ptr<conf::DashWidgetText const> v =
    std::dynamic_pointer_cast<conf::DashWidgetText const>(v_);

  if (!v) {
    qWarning("DashboardWidgetText::setValue: not a conf::DashWidgetText?");
    return false;
  }

  text->setHtml(v->text);

  return true;
}

std::shared_ptr<conf::Value const> DashboardWidgetText::getValue() const
{
  std::shared_ptr<conf::DashWidgetText> ret =
    std::make_shared<conf::DashWidgetText>(text->document()->toHtml());
  return std::static_pointer_cast<conf::Value>(ret);
}
