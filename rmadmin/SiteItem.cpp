#include <QMarginsF>
#include "GraphView.h"
#include "GraphModel.h"
#include "ProgramItem.h"
#include "SiteItem.h"

SiteItem::SiteItem(
  GraphItem *treeParent, std::unique_ptr<Site> site,
  GraphViewSettings const *settings) :
  GraphItem(treeParent, std::move(site), settings)
{
  setZValue(1);
}

void SiteItem::reorder(GraphModel const *model)
{
  for (int i = 0; (size_t)i < programs.size(); i++) {
    if (programs[i]->row != i) {
      programs[i]->row = i;
      programs[i]->setPos(30, i * 90);
      emit model->positionChanged(
        model->createIndex(i, 0, (programs[i])));
    }
  }
  prepareGeometryChange();
}

std::vector<std::pair<QString const, QString const>> SiteItem::labels() const
{
  std::shared_ptr<Site> shr =
    std::static_pointer_cast<Site>(shared);

  return {
    { "#programs", QString::number(programs.size()) },
    { "master", shr->isMaster ?
        (*shr->isMaster ? tr("yes") : tr("no")) : tr("unknown") }
  };
}

QRectF SiteItem::operationRect() const
{
  QRectF bbox;
  for (auto const &program : programs) {
    QRectF b = program->operationRect();
    b.translate(program->pos());
    bbox |= b;
  }
  bbox += QMarginsF(settings->programMarginHoriz,
                    settings->programMarginTop,
                    settings->programMarginHoriz,
                    settings->programMarginBottom);
  return bbox;
}

SiteItem::operator QString() const
{
  QString s("Site[");
  s += row;
  s += "]:";
  s += shared->name;
  for (ProgramItem const *program : programs) {
    s += *program;
  }
  return s;
}
