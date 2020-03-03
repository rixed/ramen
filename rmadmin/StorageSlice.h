#ifndef STORAGESLICE_H_190523
#define STORAGESLICE_H_190523
#include <cassert>
#include <vector>
#include <QColor>
#include <QPieSlice>

using namespace QtCharts;

struct Key {
  QString name[3];

  void reset() {
    for (unsigned r = 0; r < 3; r++) name[r].clear();
  }

  bool isValid() const
  {
    return name[0].length() || name[1].length() || name[2].length();
  }

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

enum DataMode { AllocedBytes, CurrentBytes };

struct Values {
  int64_t current, allocated;

  int64_t forMode(DataMode mode) const {
    switch (mode) {
      case CurrentBytes:
        return current;
      case AllocedBytes:
        return allocated;
    }
    assert(!"Invalid mode");
    return 0;
  }
  void operator+=(Values const &other) {
    current += other.current;
    allocated += other.allocated;
  }
};

class StorageSlice : public QPieSlice {
  Q_OBJECT

  QColor color, highColor;
  std::vector<StorageSlice *> children;
  QString longLabel, shortLabel;

  bool labelNormallyVisible;

public:
  StorageSlice(QColor, bool labelVisible, Key, Values, DataMode, QObject *parent = nullptr);
  void addChild(StorageSlice *);

  Key key;
  Values val;

public slots:
  void setSelected(bool selected = true);
};

#endif
