#include <memory>
#include <cassert>
#include <QString>
#include "FunctionItem.h"
#include "conf.h"
#include "confValue.h"
#include "RamenType.h"
#include "ChartDataSet.h"

ChartDataSet::ChartDataSet(FunctionItem const *functionItem_, unsigned column_, QObject *parent) :
  QObject(parent), functionItem(functionItem_), column(column_), isFactor(false)
{
  std::shared_ptr<RamenType const> outType = functionItem->outType();
  type = outType->structure->columnType(column);
  name_ = outType->structure->columnName(column);
  if (! type) {
    std::cerr << "Cannot find type for column " << name_.toStdString() << std::endl;
    assert(type);
  }

  // Retrieve whether this column is a factor:
  CompiledFunctionInfo const *func = functionItem->compiledInfo();
  for (QString factor : func->factors) {
    if (factor == name_) {
      isFactor = true;
      break;
    }
  }

  // Relay FunctionItem signal about addition of a tuple
  connect(functionItem, &FunctionItem::endAddTuple, this, &ChartDataSet::valueAdded);
}

bool ChartDataSet::isNumeric() const
{
  return type->structure->isNumeric();
}

unsigned ChartDataSet::numRows() const
{
  return functionItem->tuples.size();
}

RamenValue const *ChartDataSet::value(unsigned row) const
{
  assert(row < numRows());
  return functionItem->tuples[row]->columnValue(column);
}

QString ChartDataSet::name() const
{
  return name_;
}
