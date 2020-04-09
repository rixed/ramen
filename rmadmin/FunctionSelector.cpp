#include <QModelIndex>
#include <QTreeView>
#include "FunctionItem.h"
#include "GraphModel.h"

#include "FunctionSelector.h"
/* A TreeComboBox specialized for picking a function worker */

FunctionSelector::FunctionSelector(GraphModel *model, QWidget *parent)
  : TreeComboBox(parent),
    previous(nullptr)
{
  setModel(model);
  setAllowNonLeafSelection(false);

  /* Hide all columns but the name: */
  for (int c = 0; c < GraphModel::Columns::NumColumns; c++) {
    if (c != GraphModel::Columns::Name)
      treeView->hideColumn(c);
  }

  // Does not seem to do anything:
  setSizeAdjustPolicy(QComboBox::AdjustToContents);

  // This does the trick:
  connect(model, &GraphModel::rowsInserted,
          this, &FunctionSelector::resizeToContent);
  resizeToContent();

  connect(this, QOverload<int>::of(&QComboBox::currentIndexChanged),
          this, &FunctionSelector::filterSelection);
}

FunctionItem *FunctionSelector::getCurrent() const
{
  QModelIndex const index(TreeComboBox::getCurrent());

  if (! index.isValid()) return nullptr;

  GraphItem *item(static_cast<GraphItem *>(index.internalPointer()));
  FunctionItem *function(dynamic_cast<FunctionItem *>(item));
  return function;
}

void FunctionSelector::filterSelection()
{
  FunctionItem *current = getCurrent();

  if (current == previous) return;

  treeView->resizeColumnToContents(0);
  previous = current;
  emit selectionChanged(current);
}

void FunctionSelector::resizeToContent()
{
  int const width { view()->sizeHint().width() };
  view()->setMinimumWidth(width);
}
