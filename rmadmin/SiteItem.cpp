#include "ProgramItem.h"
#include "SiteItem.h"
#include "OperationsModel.h"

SiteItem::SiteItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::green),
  name(name_)
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

void SiteItem::setProperty(QString const &p, std::shared_ptr<conf::Value const> v)
{
  if (p == "is_master") {
    std::shared_ptr<conf::Bool const> b =
      std::dynamic_pointer_cast<conf::Bool const>(v);
    if (b) isMaster = b->b;
  }
}
