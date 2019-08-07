#include <QStackedLayout>
#include <QKeyEvent>
#include <QLineEdit>
#include "ProcessesWidget.h"
#include "ProcessesDialog.h"

ProcessesDialog::ProcessesDialog(GraphModel *graphModel, QWidget *parent) :
  QDialog(parent)
{
  processesWidget = new ProcessesWidget(graphModel, this);

  QStackedLayout *layout = new QStackedLayout;
  // TODO: A search box?
  layout->addWidget(processesWidget);
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  setWindowTitle(tr("Processes List"));
  setSizeGripEnabled(true);
}

void ProcessesDialog::keyPressEvent(QKeyEvent *event)
{
  if (event->key() == Qt::Key_Escape &&
      processesWidget->searchFrame->isVisible())
  {
    processesWidget->searchFrame->hide();
    processesWidget->searchBox->clear();
    event->accept();
  } else {
    QDialog::keyPressEvent(event);
  }
}
