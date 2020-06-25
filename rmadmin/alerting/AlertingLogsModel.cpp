#include <QDebug>
#include <QModelIndex>
#include <QVariant>
#include "alerting/tools.h"
#include "confValue.h"

#include "alerting/AlertingLogsModel.h"

static bool const verbose { false };

AlertingLogsModel *AlertingLogsModel::globalLogsModel;

AlertingLogsModel::AlertingLogsModel(QObject *parent)
  : QAbstractTableModel(parent)
{
  journal.reserve(20);

  iterIncidents([this](std::string const &incidentId) {
    iterLogs(incidentId, [this, &incidentId]
             (double time, std::shared_ptr<conf::IncidentLog const> log) {
      addLog(incidentId, time, log);
    });
  });

  connect(kvs, &KVStore::keyChanged,
          this, &AlertingLogsModel::onChange);
}

void AlertingLogsModel::addLog(
  std::string const &incidentId,
  double time,
  std::shared_ptr<conf::IncidentLog const> log)
{
  /* Insert it into the journal (most of the time, will be merely appended): */
  size_t i;
  for (i = 0; i < journal.size(); i++) {
    if (journal[i].time < time) break;
  }
  // Insert at position i:
  beginInsertRows(QModelIndex(), i, i);
  std::vector<AlertingLogsModel::Log>::const_iterator it { journal.cbegin() + i };
  journal.emplace(it, incidentId, time, log);
  endInsertRows();

  if (verbose)
    qDebug() << "AlertingLogsModel: has now" << journal.size() << "entries";
}

void AlertingLogsModel::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    if (change.op != KeyCreated) continue;

    std::string incidentId;
    double time;
    if (! parseLogKey(change.key, &incidentId, &time)) continue;

    std::shared_ptr<conf::IncidentLog const> log {
      std::dynamic_pointer_cast<conf::IncidentLog const>(change.kv.val) };
    if (! log) {
      qCritical() << "Log entry not a conf::IncidentLog for key"
                  << QString::fromStdString(change.key);
      continue;
    }

    addLog(incidentId, time, log);
  }
}

int AlertingLogsModel::rowCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return journal.size();
}

int AlertingLogsModel::columnCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return AlertingLogsModel::NUM_COLUMNS;
}

QVariant AlertingLogsModel::data(QModelIndex const &index, int role) const
{
  if (role != Qt::DisplayRole) return QVariant();

  size_t const row { size_t(index.row()) };
  if (row >= journal.size()) return QVariant();

  switch (static_cast<AlertingLogsModel::Columns>(index.column())) {
    case AlertingLogsModel::Time:
      return journal[row].timeStr;
    case AlertingLogsModel::Text:
      return journal[row].log->text;
    default:
      return QVariant();
  }
}

QVariant AlertingLogsModel::headerData(
  int section, Qt::Orientation orientation, int role) const
{
  if (role != Qt::DisplayRole) return QVariant();
  if (orientation != Qt::Horizontal) return QVariant();

  switch (section) {
    case 0:
      return QString(tr("Time"));
    case 1:
      return QString(tr("Text"));
    default:
      return QVariant();
  }
}
