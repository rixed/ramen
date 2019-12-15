#include <QHeaderView>
#include <QTreeView>
#include "TreeComboBox.h"

TreeComboBox::TreeComboBox(QWidget *parent)
  : QComboBox(parent),
    m_allowNonLeafSelection(true)
{
  treeView = new QTreeView;
  treeView->setFrameShape(QFrame::NoFrame);
  treeView->setSelectionBehavior(QTreeView::SelectRows);
  treeView->setRootIsDecorated(false);
  treeView->setWordWrap(true);
  treeView->setAllColumnsShowFocus(true);
  treeView->setItemsExpandable(false);
  treeView->header()->setVisible(false);
  setView(treeView);
}

void TreeComboBox::showPopup()
{
  treeView->expandAll();
  QComboBox::showPopup();
}

QModelIndex TreeComboBox::getCurrent() const
{
  QModelIndex index = treeView->currentIndex();

  if (! m_allowNonLeafSelection &&
      treeView->model()->rowCount(index) != 0)
    return QModelIndex();

  return index;
}
