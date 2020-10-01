#include <cassert>
#include <QDebug>
#include <QString>
#include "EventTime.h"
#include "RamenType.h"
#include "RamenValue.h"

static bool const verbose(false);

EventTime::EventTime(RamenType const &type) :
  startColumn(-1),
  stopColumn(-1)
{
  if (! type.vtyp->isScalar()) {
    for (int column = 0; column < type.vtyp->numColumns(); column ++) {
      std::shared_ptr<RamenType const> subType =
        type.vtyp->columnType(column);
      if (subType && subType->vtyp->isScalar()) {
        QString const name = type.vtyp->columnName(column);
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

bool EventTime::isValid() const
{
  return startColumn >= 0;
}

std::optional<double> EventTime::startOfTuple(RamenValue const &tuple) const
{
  RamenValue const *startVal = tuple.columnValue(startColumn);
  if (! startVal) return std::nullopt;
  return startVal->toDouble();
}

std::optional<double> EventTime::stopOfTuple(RamenValue const &tuple) const
{
  RamenValue const *stopVal = tuple.columnValue(stopColumn);
  if (! stopVal) return std::nullopt;
  return stopVal->toDouble();
}

QDebug operator<<(QDebug debug, EventTime const &v)
{
  QDebugStateSaver saver(debug);
  debug.nospace() << "Start column:" << v.startColumn
                  << ", Stop column:" << v.stopColumn;

  return debug;
}
