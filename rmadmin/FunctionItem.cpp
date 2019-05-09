#include "FunctionItem.h"

FunctionItem::FunctionItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::blue),
  name(name_)
{}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}
