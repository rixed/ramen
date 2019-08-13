#ifndef CHARTDATASET_H_190526
#define CHARTDATASET_H_190526
#include <memory>
#include <QObject>

class TailModel;
struct RamenType;
struct RamenValue;

class ChartDataSet : public QObject
{
  Q_OBJECT

public:
  std::shared_ptr<TailModel const> tailModel;
  QString name_;

  unsigned column;
  bool isFactor;

  ChartDataSet(
    std::shared_ptr<TailModel const>, unsigned column, QObject *parent = nullptr);
  bool isNumeric() const;
  unsigned numRows() const;
  RamenValue const *value(unsigned row) const;
  QString name() const;

signals:
  void valueAdded() const;
};

#endif
