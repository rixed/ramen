#include <iostream>
#include <algorithm>
#include <QPainter>
#include <QFontMetrics>
#include "OperationsItem.h"
#include "OperationsModel.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "GraphView.h"
#include "SiteItem.h"

// Notice that we use our parent's subItems as the parent of the GraphItem,
// meaning all coordinates will be relative to that parent. Easier when it
// moves.
// Note also that we must initialize row with an invalid value so that
// reorder detect that it's indeed a new value when we insert the first one!
OperationsItem::OperationsItem(OperationsItem *treeParent_, QString const &name_, GraphViewSettings const *settings_, QBrush brush_) :
  QGraphicsItem(treeParent_ ?
      static_cast<QGraphicsItem *>(&treeParent_->subItems) :
      static_cast<QGraphicsItem *>(treeParent_)),
  brush(brush_),
  subItems(this),
  collapsed(true),
  settings(settings_),
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
  QFont boldFont = settings->labelsFont;
  boldFont.setBold(true);

  QFontMetrics fm(boldFont);

  QPen pen = QPen(Qt::darkGray);
  pen.setWidthF(1);
  painter->setPen(pen);

  int y = settings->labelsLineHeight;
  int const x = fm.width(" ");
  for (auto label : labels) {
    painter->setFont(boldFont);
    QString const title(label.first + QString(": "));
    painter->drawText(x, y, title);
    int const x2 = x + fm.width(title);
    painter->setFont(settings->labelsFont);
    painter->drawText(x2, y, label.second);
    y += settings->labelsLineHeight;
  }
}

QRect OperationsItem::labelsBoundingRect(std::vector<std::pair<QString const, QString const>> const &labels) const
{
  QFont font = settings->labelsFont;
  font.setBold(true);

  int const x = settings->labelsFontMetrics.width(" ");
  int totWidth = 0;
  for (auto label : labels) {
    QString const totLine(label.first + QString(": ") + label.second);
    totWidth =
      std::max(totWidth, x + settings->labelsFontMetrics.width(totLine));
  }

  int const totHeight = labels.size() * settings->labelsLineHeight;

  return QRect(QPoint(0, 0), QSize(totWidth, totHeight));
}

// Every node in the graph start by displaying a set of properties:
void OperationsItem::paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *)
{
  (void)option;

  std::vector<std::pair<QString const, QString const>> labels;
  labels.reserve(9);
  labels.emplace_back("name", name);
  if (collapsed) addLabels(&labels);

  // Get the total bbox:
  QRectF bbox = boundingRect();
  QBrush bgBrush = brush;
  QColor bgColor = bgBrush.color();
  if (! collapsed) bgColor.setAlpha(10);
  bgBrush.setColor(bgColor);
  painter->setBrush(bgBrush);
  painter->drawRoundRect(bbox, 5);

  // Print labels on top:
  paintLabels(painter, labels);
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
