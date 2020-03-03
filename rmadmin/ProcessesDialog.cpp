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
  SavedWindow("ProcessesWindow", tr("Processes List"), true, parent)
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

    // Also list unused workers (from lazy functions):
    QAction *viewUnused =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Unused"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewUnused);
    viewUnused->setCheckable(true);
    viewUnused->setChecked(false);
    processesWidget->proxyModel->viewUnused(false);

    // Also list temporarily disabled functions:
    QAction *viewDisabled =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Disabled"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewDisabled);
    viewDisabled->setCheckable(true);
    viewDisabled->setChecked(true);
    processesWidget->proxyModel->viewDisabled(true);

    /* Also list workers with no pid (such as conditionally disabled workers).
     * Notice that unused or disabled workers won't have a pid. */
    QAction *viewNonRunning =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Non-Running"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewNonRunning);
    viewNonRunning->setCheckable(true);
    viewNonRunning->setChecked(false);
    processesWidget->proxyModel->viewNonRunning(false);

    // Also list instances without any worker at all:
    QAction *viewFinished =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Finished"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewFinished);
    viewFinished->setCheckable(true);
    viewFinished->setChecked(false);
    processesWidget->proxyModel->viewFinished(false);

    QAction *viewTopHalves =
      viewMenu->addAction(
        QCoreApplication::translate("QMenuBar", "Top-Halves"),
        processesWidget->proxyModel, &ProcessesWidgetProxy::viewTopHalves);
    viewTopHalves->setCheckable(true);
    viewTopHalves->setChecked(false);
    processesWidget->proxyModel->viewTopHalves(false);

    viewMenu->addSeparator();
    for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
      if (c == GraphModel::ActionButton1 ||
          c == GraphModel::ActionButton2) continue; // Name and buttons are mandatory

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

    // Does not really matter at that point but pleases static analyser:
    processesWidget = nullptr;
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
