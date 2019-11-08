#include <QTreeView>
#include <QLabel>
#include <QLineEdit>
#include <QVBoxLayout>
#include "SavedWindow.h"
#include "NamesTree.h"
#include "NamesTreeWin.h"

NamesTreeWin::NamesTreeWin(QWidget *parent) :
  SavedWindow("Completable Names", tr("Completable Names"), true, parent)
{
  if (NamesTree::globalNamesTree) {
    QLineEdit *lineEdit = new QLineEdit;
    // Or use the completer on a NamesSubtree:
    QCompleter *completer = new NamesCompleter(NamesTree::globalNamesTree);
    lineEdit->setCompleter(completer);

    QTreeView *treeWidget = new QTreeView;
    treeWidget->setModel(NamesTree::globalNamesTree);
    treeWidget->setHeaderHidden(true);
    treeWidget->setUniformRowHeights(true);

    QVBoxLayout *layout = new QVBoxLayout;
    layout->addWidget(lineEdit);
    layout->addWidget(treeWidget);
    QWidget *widget = new QWidget;
    widget->setLayout(layout);
    setCentralWidget(widget);
  } else {
    QString errMsg(tr("No names tree yet!?"));
    setCentralWidget(new QLabel(errMsg));
    // Better luck next time?
    setAttribute(Qt::WA_DeleteOnClose);
  }
}
