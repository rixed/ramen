#include <QPushButton>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include "dashboard/DashboardTextEditor.h"

#include "dashboard/DashboardWidgetText.h"

DashboardWidgetText::DashboardWidgetText(
  std::string const &key,
  QWidget *parent)
  : DashboardWidget(key, parent)
{
  editor = new DashboardTextEditor;
  addWidget(editor, true);
  editor->setKey(key);

  QHBoxLayout *buttonsLayout = new QHBoxLayout;
  buttonsLayout->addStretch();
  buttonsLayout->addWidget(cancelButton);
  buttonsLayout->addWidget(submitButton);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(editor);
  layout->addLayout(buttonsLayout);
  widget = new QWidget(this);
  widget->setLayout(layout);

  cancelButton->setVisible(false);
  submitButton->setVisible(false);
  /* Open/close the editor when the AtomicForm is enabled/disabled: */
  connect(this, &DashboardWidgetText::changeEnabled,
          this, [this](bool enabled) {
    cancelButton->setVisible(enabled);
    submitButton->setVisible(enabled);
  });

  setCentralWidget(widget);
}

AtomicWidget *DashboardWidgetText::atomicWidget() const
{
  return editor;
}
