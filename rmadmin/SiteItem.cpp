#include <QMarginsF>
#include "ProgramItem.h"
#include "SiteItem.h"
#include "GraphView.h"
#include "GraphModel.h"

SiteItem::SiteItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *settings) :
  GraphItem(treeParent, name, settings)
{
  setZValue(1);
}

SiteItem::~SiteItem()
{
  for (ProgramItem *program : programs) {
    delete program;
  }
}

void SiteItem::reorder(GraphModel const *model)
{
  for (int i = 0; (size_t)i < programs.size(); i++) {
    if (programs[i]->row != i) {
      programs[i]->row = i;
      programs[i]->setPos(30, i * 90);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<GraphItem *>(programs[i])));
    }
  }
  prepareGeometryChange();
}

std::vector<std::pair<QString const, QString const>> SiteItem::labels() const
{
  return {
    { "#programs", QString::number(programs.size()) },
    { "master", isMaster ? (*isMaster ? tr("yes") : tr("no")) : tr("unknown") }
  };
}

QRectF SiteItem::operationRect() const
{
  QRectF bbox;
  for (auto program : programs) {
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

