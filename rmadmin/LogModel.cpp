#include <cassert>
#include <QDateTime>
#include "misc.h"
#include "Logger.h"
#include "LogModel.h"

#ifdef QT_NO_DEBUG_OUTPUT
# define LOG_HISTORY 5000
#else
# define LOG_HISTORY 50000
#endif

LogModel::LogModel(QObject *parent)
  : QAbstractListModel(parent),
    first(0), removingFirst(false)
{
  logs.reserve(LOG_HISTORY);
}

QVariant LogModel::data(const QModelIndex &index, int role) const
{
  if (index.row() >=
        (removingFirst ? logs.count() - 1 : logs.count()))
    return QVariant();

  if (index.column() >= 3) return QVariant();

  int const i =
      (index.row() + first + (removingFirst ? 1 : 0)) % logs.count();

  switch (index.column()) {
    case 0:
      switch (role) {
        case Qt::DisplayRole: return logs[i].time;
        default: return QVariant();
      }
      break;

    case 1:
      switch (role) {
        case Qt::DisplayRole: return logLevel(logs[i].type);
        default: return QVariant();
      }
      break;

    case 2:  // Message
      switch (role) {
        case Qt::DisplayRole: return logs[i].msg;
        case Qt::UserRole: return logs[i].type;
        default: return QVariant();
      }
      break;

    default:
      return QVariant();
  }
}

QVariant LogModel::headerData(int section, Qt::Orientation orientation, int role) const
{
  if (orientation != Qt::Horizontal) return QVariant();
  if (role != Qt::DisplayRole) return QVariant();

  switch (section) {
    case 0: return QString(tr("Time"));
    case 1: return QString(tr("Level"));
    case 2: return QString(tr("Message"));
    default: return QVariant();
  }
}

int LogModel::rowCount(const QModelIndex &index) const
{
  if (index.isValid()) return 0;
  assert(logs.count() > 0 || !removingFirst);
  return logs.count() - (removingFirst ? 1 : 0);
}

void LogModel::append(QtMsgType type, QString const &msg_)
{
  int const count = logs.count();
  static int const maxLen = 2000;
  QString const msg(msg_.length() < maxLen ? msg_ : msg_.left(maxLen-1) + "…");

  if (count < LOG_HISTORY) {
    beginInsertRows(QModelIndex(), count, count);
    QDateTime time(QDateTime::currentDateTime());
    logs.append({ time, type, msg });
    endInsertRows();
  } else {
    beginRemoveRows(QModelIndex(), 0, 0);
    removingFirst = true;
    endRemoveRows();

    beginInsertRows(QModelIndex(), count - 1, count - 1);
    removingFirst = false;
    logs[first].type = type;
    logs[first].msg = msg;
    first = (first + 1) % count;
    endInsertRows();
  }
}
