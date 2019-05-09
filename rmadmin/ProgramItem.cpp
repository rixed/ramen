#include "FunctionItem.h"
#include "ProgramItem.h"
#include "OperationsModel.h"

ProgramItem::ProgramItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::red), name(name_) {}

ProgramItem::~ProgramItem()
{
  for (FunctionItem *function : functions) {
    delete function;
  }
}

QVariant ProgramItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void ProgramItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < functions.size(); i++) {
    if (functions[i]->row != i) {
      functions[i]->row = i;
      functions[i]->setPos(30, i * 30);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(functions[i])));
    }
  }
}
