#include <memory>
#include <cassert>
#include <QString>
#include "FunctionItem.h"
#include "conf.h"
#include "confValue.h"
#include "serValue.h"
#include "RamenType.h"
#include "ChartDataSet.h"

ChartDataSet::ChartDataSet(FunctionItem const *functionItem_, unsigned column_, QObject *parent) :
  QObject(parent), functionItem(functionItem_), column(column_), isFactor(false)
{
  std::shared_ptr<RamenType const> outType = functionItem->outType();
  type = outType->columnType(column);
  name_ = outType->columnName(column);
  QString const name = outType->columnName(column);

  // Retrieve whether this column is a factor:
  CompiledFunctionInfo const *func = functionItem->compiledInfo();
  for (QString factor : func->factors) {
    if (factor == name) {
      isFactor = true;
      break;
    }
  }

  // Relay FunctionItem signal about addition of a tuple
  connect(functionItem, &FunctionItem::endAddTuple, this, &ChartDataSet::valueAdded);
}

bool ChartDataSet::isNumeric() const
{
  return type->isNumeric();
}

unsigned ChartDataSet::numRows() const
{
  return functionItem->tuples.size();
}

ser::Value const *ChartDataSet::value(unsigned row) const
{
  assert(row < numRows());
  return functionItem->tuples[row]->columnValue(column);
}

QString ChartDataSet::name() const
{
  return name_;
}
