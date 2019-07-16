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
  conf::kvs_lock.lock_shared();
  for (unsigned i = 0; i < 1000; i++) {
    conf::Key k = functionItem->functionKey("/factors/" + std::to_string(i));
    auto const &it = conf::kvs.find(k);
    if (it == conf::kvs.end()) break;
    std::shared_ptr<conf::Value const> v_(it.value().val);
    std::shared_ptr<conf::RamenValueValue const> v =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(v_);
    if (! v) {
      std::cout << "Factor #" << i << " is not a string!?" << std::endl;
      continue;
    }
    if (v->toQString() == name) {
      isFactor = true;
      break;
    }
  }
  conf::kvs_lock.unlock_shared();

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
