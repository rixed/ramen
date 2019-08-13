#include <memory>
#include <cassert>
#include <QString>
#include "TailModel.h"
#include "conf.h"
#include "confValue.h"
#include "RamenType.h"
#include "ChartDataSet.h"

ChartDataSet::ChartDataSet(
  std::shared_ptr<TailModel const> tailModel_,
  unsigned column_, QObject *parent) :
  QObject(parent),
  tailModel(tailModel_),
  column(column_), isFactor(false)
{
  name_ = tailModel->type->structure->columnName(column);

  // Retrieve whether this column is a factor:
  for (QString factor : tailModel->factors) {
    if (factor == name_) {
      isFactor = true;
      break;
    }
  }

  // Relay Function signal about addition of a tuple
  connect(tailModel.get(), &TailModel::rowsInserted,
          this, &ChartDataSet::valueAdded);
}

bool ChartDataSet::isNumeric() const
{
  return
    tailModel->type->structure->columnType(column)->structure->isNumeric();
}

unsigned ChartDataSet::numRows() const
{
  return tailModel->rowCount();
}

RamenValue const *ChartDataSet::value(unsigned row) const
{
  assert(row < numRows());
  return tailModel->tuples[row]->columnValue(column);
}

QString ChartDataSet::name() const
{
  return name_;
}
