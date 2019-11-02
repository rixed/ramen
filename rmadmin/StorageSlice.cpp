#include "StorageSlice.h"

using namespace QtCharts;

static QString labelOfKey(Key k)
{
  QString label;
  for (unsigned r = 0; r < 3; r++) {
    if (k.name[r].length() == 0) continue;
    if (label.length()) label.append("/");
    label.append(k.name[r]);
  }
  return label;
}

StorageSlice::StorageSlice(QColor color_, bool labelVisible_, Key key_, Values val_, DataMode dataMode, QObject *parent) :
  QPieSlice(labelOfKey(key_), val_.forMode(dataMode), parent),
  color(color_),
  labelNormallyVisible(labelVisible_),
  key(key_),
  val(val_)
{
  longLabel = label();
  for (unsigned r = 0; r < 3; r++) {
    if (key.name[r].length()) shortLabel = key.name[r];
  }

  setColor(color);
  highColor = color.lighter();

  setLabelVisible(labelNormallyVisible);

  setExplodeDistanceFactor(0.04);
}

void StorageSlice::addChild(StorageSlice *slice)
{
  children.push_back(slice);
}

void StorageSlice::setSelected(bool s)
{
  setColor(s ? highColor : color);
  setLabelVisible(s || labelNormallyVisible);
  setLabel(s ? shortLabel : longLabel);
  setExploded(s);
  for (auto slice : children) {
    slice->setExploded(s);
    slice->setLabelVisible(!s && slice->labelNormallyVisible);
  }
}
