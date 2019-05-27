#ifndef CHARTDATASET_H_190526
#define CHARTDATASET_H_190526
#include <QObject>

class FunctionItem;
namespace conf {
  struct RamenType;
};

class ChartDataSet : public QObject
{
  Q_OBJECT

  FunctionItem const *functionItem;
  std::shared_ptr<conf::RamenType const> type;

  unsigned column;
  bool isFactor;

public:
  ChartDataSet(FunctionItem const *, unsigned column, QObject *parent = nullptr);
  bool isNumeric() const;
};

#endif
