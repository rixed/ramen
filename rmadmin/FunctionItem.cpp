#include "FunctionItem.h"

FunctionItem::FunctionItem(OperationsItem *parent, QString const &name) :
  OperationsItem(parent, name, Qt::blue)
{}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}
