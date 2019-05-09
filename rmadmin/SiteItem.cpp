#include "ProgramItem.h"
#include "SiteItem.h"
#include "OperationsModel.h"

SiteItem::SiteItem(OperationsItem *parent, QString const &name) :
  OperationsItem(parent, name, Qt::green)
{}

SiteItem::~SiteItem()
{
  for (ProgramItem *program : programs) {
    delete program;
  }
}

QVariant SiteItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void SiteItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < programs.size(); i++) {
    if (programs[i]->row != i) {
      programs[i]->row = i;
      programs[i]->setPos(30, i * 90);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(programs[i])));
    }
  }
}
