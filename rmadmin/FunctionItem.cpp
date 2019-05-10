#include "FunctionItem.h"

FunctionItem::FunctionItem(OperationsItem *treeParent, QString const &name) :
  OperationsItem(treeParent, name, Qt::blue)
{
  updateFrame();
}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

std::vector<std::pair<QString const, QString const>> FunctionItem::graphLabels() const
{
  return {
    { "name", name }
  };
}
