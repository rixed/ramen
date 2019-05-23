#ifndef STORAGESLICE_H_190523
#define STORAGESLICE_H_190523
#include <vector>
#include <QPieSlice>
#include <QColor>

using namespace QtCharts;

struct Key {
  QString name[3];

  bool isValid() { return name[0].length() || name[1].length() || name[2].length(); }
  Key &operator=(Key const &rhs)
  {
    for (unsigned r = 0; r < 3; r ++) name[r] = rhs.name[r];
    return *this;
  }
  bool operator==(Key const &other) const
  {
    for (unsigned r = 0; r < 3; r ++)
      if (name[r] != other.name[r]) return false;
    return true;
  }
};

struct KeyCompare {
  bool operator()(Key const &a, Key const &b) const
  {
    return
      a.name[0] < b.name[0] || (
        a.name[0] == b.name[0] && (
          a.name[1] < b.name[1] || (
            a.name[1] == b.name[1] &&
              a.name[2] < b.name[2])));
  }
};

class StorageSlice : public QPieSlice {
  Q_OBJECT

  QColor color, highColor;
  std::vector<StorageSlice *> children;
  QString longLabel, shortLabel;

public:
  StorageSlice(QColor, bool labelVisible, Key, qreal value, QObject *parent = nullptr);
  void addChild(StorageSlice *);

  bool labelNormallyVisible;
  Key key;

public slots:
  void setSelected(bool selected = true);
};

#endif
