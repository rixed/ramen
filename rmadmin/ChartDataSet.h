#ifndef CHARTDATASET_H_190526
#define CHARTDATASET_H_190526
#include <memory>
#include <QObject>

class Function;
struct RamenType;
struct RamenValue;

class ChartDataSet : public QObject
{
  Q_OBJECT

  std::shared_ptr<Function const> function;
  std::shared_ptr<RamenType const> type;
  QString name_;

  unsigned column;
  bool isFactor;

public:
  ChartDataSet(
    std::shared_ptr<Function const>, unsigned column, QObject *parent = nullptr);
  bool isNumeric() const;
  unsigned numRows() const;
  RamenValue const *value(unsigned row) const;
  QString name() const;

signals:
  void valueAdded() const;
};

#endif
