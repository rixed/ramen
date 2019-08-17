#include <QTreeView>
#include <QLabel>
#include "SavedWindow.h"
#include "NamesTree.h"
#include "NamesTreeWin.h"

NamesTreeWin::NamesTreeWin(QWidget *parent) :
  SavedWindow("Completable Names", tr("Completable Names"), parent)
{
  if (NamesTree::globalNamesTree) {
    QTreeView *treeWidget = new QTreeView(this);
    treeWidget->setModel(NamesTree::globalNamesTree);
    setCentralWidget(treeWidget);
  } else {
    QString errMsg(tr("No names tree yet!?"));
    setCentralWidget(new QLabel(errMsg));
    // Better luck next time?
    setAttribute(Qt::WA_DeleteOnClose);
  }
}
