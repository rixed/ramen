#include <algorithm>
#include <QPainter>
#include <QFontMetrics>
#include <QPropertyAnimation>
#include "GraphItem.h"
#include "GraphModel.h"
#include "GraphView.h"
#include "colorOfString.h"

/* The dummbest QGraphicsItem I can made. Does nothing, paint nothing,
 * but can be used to hide/move/transform its children in one go.
 * And unlike QGrpahicsItemGroup, will not hide events.
 * Very useful, should be in Qt IMHO. */
class GraphicsEmpty : public QAbstractGraphicsShapeItem
{
public:
  GraphicsEmpty(QGraphicsItem *parent = nullptr) :
    QAbstractGraphicsShapeItem (parent) {}
  QRectF boundingRect() const { return QRectF(); }
  void paint(QPainter *, const QStyleOptionGraphicsItem *, QWidget * = nullptr) {}
};

// Notice that we use our parent's subItems as the parent of the GraphItem,
// meaning all coordinates will be relative to that parent. Easier when it
// moves.
// Note also that we must initialize row with an invalid value so that
// reorder detect that it's indeed a new value when we insert the first one!
GraphItem::GraphItem(
  GraphItem *treeParent_, std::unique_ptr<GraphData> data,
  GraphViewSettings const *settings_) :
  QGraphicsItem(treeParent_ ? treeParent_->subItems : treeParent_),
  border_(2),
  collapsed(true),
  settings(settings_),
  x0(0), y0(0), x1(0), y1(0),
  xRank(0), yRank(0),
  treeParent(treeParent_),
  row(-1),
  shared(std::move(data))
{
  brush = QBrush(colorOfString(shared->name)),

  // Notifies itemChange whenever the position is changed:
  setFlag(ItemSendsGeometryChanges, true);
  // or the item (un)selected:
  setFlag(ItemIsSelectable, true);
  setAcceptTouchEvents(true);

  // TreeView is initially collapsed, and so are we:
  subItems = new GraphicsEmpty(this);
  subItems->hide();
}

QVariant GraphItem::data(int column, int role) const
{
  if (role != Qt::DisplayRole) return QVariant();
  if (column != 0) return QVariant();
  return shared->name;
}

bool GraphItem::isCollapsed() const
{
  return collapsed;
}

void GraphItem::setCollapsed(bool c)
{
  collapsed = c;
  subItems->setVisible(!c);
}

QString GraphItem::fqName() const
{
  if (! treeParent) return shared->name;
  return treeParent->fqName() + "/" + shared->name;
}

QColor GraphItem::color() const
{
  QColor c = brush.color();
  c.setAlpha(collapsed ? 200 : 25);
  return c;
}

qreal GraphItem::border() const
{
  return border_;
}

void GraphItem::setBorder(qreal b)
{
  if (b != border_) {
    border_ = b;
    update();
  }
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
 * The upper-left corner of that frame lays at (0, 0). */

void GraphItem::paintLabels(QPainter *painter, std::vector<std::pair<QString const, QString const>> const &labels, int y)
{
  QFont boldFont = settings->labelsFont;
  boldFont.setBold(true);
  QFontMetrics fm(boldFont);

  QPen pen = QPen(Qt::black);
  pen.setWidthF(0);
  painter->setPen(pen);

  y += settings->labelsLineHeight;
  int const x = settings->labelsHorizMargin;
  for (auto const &label : labels) {
    int x2 = x;
    painter->setFont(boldFont);
    if (label.first.length() > 0) {
      QString const title(label.first + QString(": "));
      painter->drawText(x, y, title);
      x2 += fm.boundingRect(title).width();
      painter->setFont(settings->labelsFont);
    }
    painter->drawText(x2, y, label.second);
    y += settings->labelsLineHeight;
  }
}

QRect GraphItem::labelsBoundingRect(std::vector<std::pair<QString const, QString const>> const &labels) const
{
  QFont font = settings->labelsFont;
  font.setBold(true);

  QFontMetrics fm(font);

  int totWidth = 0;
  for (auto const &label : labels) {
    QString const totLine(label.first + QString(": ") + label.second);
    totWidth =
      std::max(totWidth, settings->labelsHorizMargin + fm.boundingRect(totLine).width());
  }

  int const totHeight = labels.size() * settings->labelsLineHeight;

  return QRect(QPoint(0, 0), QSize(totWidth, totHeight));
}

QModelIndex GraphItem::index(GraphModel const *model, int column) const
{
  return model->index(row, column,
    treeParent ? treeParent->index(model, 0) : QModelIndex());
}

int GraphItem::columnCount() const
{
  return GraphModel::NumColumns;
}

QRectF GraphItem::boundingRect() const
{
  QRectF bbox = operationRect();
  qreal b = border();
  bbox += QMargins(b, b, b, b);
  return bbox;
}

// Every node in the graph start by displaying a set of properties:
void GraphItem::paint(QPainter *painter, const QStyleOptionGraphicsItem *, QWidget *)
{
  qreal b = border();
  QBrush bru = brush;
  bru.setColor(color().lighter());
  painter->setBrush(bru);
  QPen blockPen(Qt::NoBrush, b, isSelected() ? Qt::SolidLine : Qt::DashLine);
  blockPen.setColor(isSelected() ? Qt::darkGreen : Qt::darkGray);
  painter->setPen(blockPen);

  // Get the total bbox and draw inside:
  QRectF bbox = boundingRect();
  bbox -= QMargins(b, b, b, b);
  painter->drawRoundedRect(bbox, 5, 5);

  // Title (ie name)
  painter->setFont(settings->titleFont);

  QPen titlePen = QPen(Qt::black);
  titlePen.setWidth(0);
  painter->setPen(titlePen);

  painter->drawText(
    settings->labelsHorizMargin, settings->titleLineHeight, shared->name);

  // Labels:
  if (collapsed) {
    std::vector<std::pair<QString const, QString const>> labs = labels();
    paintLabels(painter, labs, settings->titleLineHeight);
  }
}

QVariant GraphItem::itemChange(QGraphicsItem::GraphicsItemChange change, const QVariant &v)
{
  if (treeParent && change == QGraphicsItem::ItemPositionHasChanged) {
    update();
  } else if (change == ItemSelectedChange) {
    if (v.toBool() && !isCollapsed()) return false;
    return v;
  } else if (change == ItemSelectedHasChanged) {
    if (v.toBool()) {
      // This one is selected
      setBorder(14);
      ensureVisible();
      QPropertyAnimation *borderAnim = new QPropertyAnimation(this, "border");
      borderAnim->setDuration(200);
      borderAnim->setEndValue(4);
      borderAnim->start(QAbstractAnimation::DeleteWhenStopped);
    } else {
      // This one is unselected
      setBorder(2);
    }
  }
  return v;
}
