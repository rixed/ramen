#include <cassert>
#include <QDebug>
#include <QString>
#include "EventTime.h"
#include "RamenType.h"
#include "RamenValue.h"

static bool verbose = true;

EventTime::EventTime(RamenType const &type) :
  startColumn(-1),
  stopColumn(-1)
{
  if (! type.structure->isScalar()) {
    for (int column = 0; column < type.structure->numColumns(); column ++) {
      std::shared_ptr<RamenType const> subType =
        type.structure->columnType(column);
      if (subType && subType->structure->isScalar()) {
        QString const name = type.structure->columnName(column);
        if (name == "start") {
          startColumn = column;
          if (verbose)
            qDebug() << "EventTime::EventTime(): Found start field to be "
                     << startColumn;
        } else if (name == "stop") {
          stopColumn = column;
          if (verbose)
            qDebug() << "EventTime::EventTime(): Found stop field to be "
                     << stopColumn;
        }
      }
    }
  }
}

std::optional<double> EventTime::ofTuple(RamenValue const &tuple) const
{
  RamenValue const *startVal = tuple.columnValue(startColumn);
  if (! startVal) return std::nullopt;
  return startVal->toDouble();
}
