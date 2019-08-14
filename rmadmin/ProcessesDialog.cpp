#include <QMenuBar>
#include <QMenu>
#include <QKeyEvent>
#include <QLineEdit>
#include <QLabel>
#include <QTreeView>
#include "GraphModel.h"
#include "ProcessesWidget.h"
#include "ProcessesWidgetProxy.h"
#include "ProcessesDialog.h"

ProcessesDialog::ProcessesDialog(QWidget *parent) :
  SavedWindow("ProcessesWindow", tr("Processes List"), parent)
{
  if (GraphModel::globalGraphModel) {

    processesWidget =
      new ProcessesWidget(GraphModel::globalGraphModel, this);
    setCentralWidget(processesWidget);

    /* The menu bar */
    QMenu *viewMenu = this->menuBar()->addMenu(
      QCoreApplication::translate("QMenuBar", "&View"));

    viewMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Search…"),
      processesWidget, &ProcessesWidget::openSearch,
      QKeySequence::Find);

    QAction *viewTopHalves =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Top-Halves"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewTopHalves);
    viewTopHalves->setCheckable(true);
    viewTopHalves->setChecked(false);
    processesWidget->proxyModel->viewTopHalves(false);

    QAction *viewStopped =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Stopped"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewStopped);
    viewStopped->setCheckable(true);
    viewStopped->setChecked(false);
    processesWidget->proxyModel->viewStopped(false);

    viewMenu->addSeparator();
    for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
      if (c == GraphModel::ActionButton) continue; // Name and buttons are mandatory

      QString const name = GraphModel::columnName((GraphModel::Columns)c);
      // Column names have already been translated
      QAction *toggle =
        viewMenu->addAction(name, [this, c](bool checked) {
          processesWidget->treeView->setColumnHidden(c, !checked);
        });
      toggle->setCheckable(true);
      if (GraphModel::columnIsImportant((GraphModel::Columns)c)) {
        toggle->setChecked(true);
      } else {
        processesWidget->treeView->setColumnHidden(c, true);
      }
    }
    /* Although it seems to be the last entry, Qt will actually add some more
     * on MacOS ("enter full screen"): */
    viewMenu->addSeparator();

  } else {

    QString errMsg(tr("No global GraphModel yet!?"));
    setCentralWidget(new QLabel(errMsg));
    // Better luck next time?
    setAttribute(Qt::WA_DeleteOnClose);

  }
}

void ProcessesDialog::keyPressEvent(QKeyEvent *event)
{
  if (! processesWidget) return;

  if (event->key() == Qt::Key_Escape &&
      processesWidget->searchFrame->isVisible())
  {
    processesWidget->searchFrame->hide();
    processesWidget->searchBox->clear();
    event->accept();
  } else {
    QMainWindow::keyPressEvent(event);
  }
}
