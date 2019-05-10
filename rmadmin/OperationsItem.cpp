#include <iostream>
#include <algorithm>
#include <QPainter>
#include <QFontMetrics>
#include "OperationsItem.h"
#include "OperationsModel.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"

// Notice that we use our parent's subItems as the parent of the GraphItem,
// meaning all coordinates will be relative to that parent. Easier when it
// moves.
// Note also that we must initialize row with an invalid value so that
// reorder detect that it's indeed a new value when we insert the first one!
OperationsItem::OperationsItem(OperationsItem *treeParent_, QString const &name_, QBrush brush_) :
  QGraphicsItem(treeParent_ ?
      static_cast<QGraphicsItem *>(&treeParent_->subItems) :
      static_cast<QGraphicsItem *>(treeParent_)),
  brush(brush_),
  subItems(this),
  collapsed(true),
  name(name_),
  treeParent(treeParent_),
  row(-1)
{
  anchorIn = new GraphAnchor(this);
  anchorOut = new GraphAnchor(this);

  // TreeView is initially collapsed, and so are we:
  subItems.hide();

  // Notify updateFrame whenever the position is changed:
  setFlag(ItemSendsGeometryChanges, true);
}

OperationsItem::~OperationsItem()
{
  delete anchorIn;
  delete anchorOut;
}

void OperationsItem::setCollapsed(bool c)
{
  collapsed = c;
  subItems.setVisible(!c);
}

QString OperationsItem::fqName() const
{
  if (! treeParent) return name;
  return treeParent->fqName() + "/" + name;
}

/*
 * CAUTION: Wet Paint!
 */

/* The layout being independent of the collapsed status (to avoid
 * re-layouting all the time) the location and size of an item is always
 * the same, whether its subItems are visible or not.
 * It consists of a frame with a set of labels on the top-left corner,
 * and some drawing (either the subItems or some stats related to the
 * item itself) inside.
 * The anchors are located at the middle of the left and right frame
 * borders.
 * The upper-left corner of that frame lays at (0, 0). */

void OperationsItem::paintLabels(QPainter *painter, std::vector<std::pair<QString const, QString const>> const &labels)
{
  QFont normalFont = painter->font();
  QFont boldFont = normalFont;
  boldFont.setBold(true);

  QFontMetrics fm(boldFont);
  int const lineHeight = fm.height() + ((fm.height() + 4) / 5);

  QPen pen = QPen(Qt::darkGray);
  pen.setWidthF(1);
  painter->setPen(pen);

  int y = lineHeight;
  int const x = fm.width(" ");
  for (auto label : labels) {
    painter->setFont(boldFont);
    QString const title(label.first + QString(": "));
    painter->drawText(x, y, title);
    int const x2 = x + fm.width(title);
    painter->setFont(normalFont);
    painter->drawText(x2, y, label.second);
    y += lineHeight;
  }
}

QRect OperationsItem::labelsBoundingRect(std::vector<std::pair<QString const, QString const>> const &labels) const
{
  QFont font = labelsFont;
  font.setBold(true);
  QFontMetrics fm(labelsFont);
  int const lineHeight = fm.height() + ((fm.height() + 4) / 5);

  int const x = fm.width(" ");
  int totWidth = 0;
  for (auto label : labels) {
    QString const totLine(label.first + QString(": ") + label.second);
    totWidth = std::max(totWidth, x + fm.width(totLine));
  }

  int const totHeight = labels.size() * lineHeight;

  return QRect(QPoint(0, 0), QSize(totWidth, totHeight));
}

// Every node in the graph start by displaying a set of properties:
void OperationsItem::paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *)
{
  (void)option;

  auto labels = graphLabels();

  // Get the total bbox:
  QRectF bbox = boundingRect();
  double x0 = bbox.x();
  double y0 = bbox.y();
  double x1 = x0 + bbox.width();
  double y1 = y0 + bbox.height();

  QBrush bgBrush = brush;
  QColor bgColor = bgBrush.color();
  bgColor.setAlpha(100);
  bgBrush.setColor(bgColor);
  painter->setBrush(bgBrush);
  painter->drawRect(bbox);

  painter->setPen(QPen(brush, 2));
  painter->drawLine(x0, y0, x0, y1);
  painter->setPen(QPen(brush, 1));
  painter->drawLine(x1, y0, x1, y1);

  // Print labels on top:
  paintLabels(painter, labels);
}

QRectF OperationsItem::boundingRect() const
{
  return labelsBoundingRect(graphLabels());
}

void OperationsItem::updateFrame()
{
  // TODO: Recompute sizes etc...

  // useful?
  prepareGeometryChange();

  // reposition anchors:
  QRectF bbox = boundingRect();
  int const midHeight = bbox.y() + bbox.height() / 2;
  anchorIn->setPos(bbox.x(), midHeight);
  anchorOut->setPos(bbox.x() + bbox.width(), midHeight);

  if (treeParent) treeParent->updateFrame();
}

QVariant OperationsItem::itemChange(QGraphicsItem::GraphicsItemChange change, const QVariant &v)
{
  if (treeParent && change == QGraphicsItem::ItemPositionHasChanged) {
    treeParent->updateFrame();
  }
  return v;
}
