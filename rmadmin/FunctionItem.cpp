#include "FunctionItem.h"

FunctionItem::FunctionItem(OperationsItem *treeParent, QString const &name) :
  OperationsItem(treeParent, name, Qt::blue)
{}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}
