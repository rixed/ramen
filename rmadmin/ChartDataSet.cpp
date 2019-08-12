#include <memory>
#include <cassert>
#include <QString>
#include "FunctionItem.h"
#include "conf.h"
#include "confValue.h"
#include "RamenType.h"
#include "ChartDataSet.h"

ChartDataSet::ChartDataSet(
  std::shared_ptr<Function const> function_,
  unsigned column_, QObject *parent) :
  QObject(parent),
  function(function_),
  column(column_), isFactor(false)
{
  std::shared_ptr<RamenType const> outType = function->outType();
  type = outType->structure->columnType(column);
  name_ = outType->structure->columnName(column);
  if (! type) {
    std::cerr << "Cannot find type for column " << name_.toStdString() << std::endl;
    assert(type);
  }

  // Retrieve whether this column is a factor:
  CompiledFunctionInfo const *func = function->compiledInfo();
  for (QString factor : func->factors) {
    if (factor == name_) {
      isFactor = true;
      break;
    }
  }

  // Relay Function signal about addition of a tuple
  connect(function.get(), &Function::endAddTuple,
          this, &ChartDataSet::valueAdded);
}

bool ChartDataSet::isNumeric() const
{
  return type->structure->isNumeric();
}

unsigned ChartDataSet::numRows() const
{
  return function->tuples.size();
}

RamenValue const *ChartDataSet::value(unsigned row) const
{
  assert(row < numRows());
  return function->tuples[row]->columnValue(column);
}

QString ChartDataSet::name() const
{
  return name_;
}
