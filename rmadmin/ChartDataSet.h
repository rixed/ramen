#ifndef CHARTDATASET_H_190526
#define CHARTDATASET_H_190526
#include <QObject>

class FunctionItem;
namespace conf {
  struct RamenType;
};
namespace ser {
  class Value;
};

class ChartDataSet : public QObject
{
  Q_OBJECT

  FunctionItem const *functionItem;
  std::shared_ptr<conf::RamenType const> type;
  QString name_;

  unsigned column;
  bool isFactor;

public:
  ChartDataSet(FunctionItem const *, unsigned column, QObject *parent = nullptr);
  bool isNumeric() const;
  unsigned numRows() const;
  ser::Value const *value(unsigned row) const;
  QString name() const;
};

#endif
