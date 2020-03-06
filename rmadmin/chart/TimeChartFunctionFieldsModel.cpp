#include <cassert>
#include <QDebug>
#include <QModelIndex>
#include <QVariant>
#include "conf.h"
#include "RamenType.h"

#include "chart/TimeChartFunctionFieldsModel.h"

static bool const verbose = false;

TimeChartFunctionFieldsModel::TimeChartFunctionFieldsModel(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  QObject *parent)
  : QAbstractTableModel(parent),
    source(site, program, function),
    infoKey(
      "sources/" + srcPathFromProgramName(program) + "/info")
{
  connect(&kvs, &KVStore::valueChanged,
          this, &TimeChartFunctionFieldsModel::resetInfo);

  // Get all numeric fields (similar to NamesTree):
  std::shared_ptr<conf::SourceInfo const> sourceInfos;
  kvs.lock.lock_shared();
  auto it = kvs.map.find(infoKey);
  if (it != kvs.map.end()) {
    sourceInfos = std::dynamic_pointer_cast<conf::SourceInfo const>(it->second.val);
    if (! sourceInfos)
      qCritical() << "TimeChartFunctionFieldsModel: Not a SourceInfo!?";
  }
  kvs.lock.unlock_shared();

  if (! sourceInfos) {
    qWarning() << "TimeChartFunctionFieldsModel: Cannot get field of"
               << QString::fromStdString(infoKey);
    return;
  }

  if (sourceInfos->isError()) {
    qWarning() << "TimeChartFunctionFieldsModel:"
               << QString::fromStdString(infoKey) << "is not compiled yet";
    return;
  }

  QString function_(QString::fromStdString(function));
  for (auto &info : sourceInfos->infos) {
    if (info->name != function_) continue;
    std::shared_ptr<RamenTypeStructure const> s(info->outType->structure);
    for (int c = 0; c < s->numColumns(); c++) {
      QString const columnName = s->columnName(c);
      std::shared_ptr<RamenType const> t(s->columnType(c));
      if (t->structure->isNumeric()) numericFields += columnName;
      if (info->factors.contains(columnName)) factors += columnName;
    }
    break;
  }
}

int TimeChartFunctionFieldsModel::rowCount(QModelIndex const &) const
{
  return numericFields.count();
}

int TimeChartFunctionFieldsModel::columnCount(QModelIndex const &) const
{
  return NumColumns;
}

conf::DashboardWidgetChart::Column const *
  TimeChartFunctionFieldsModel::findFieldConfiguration(
    std::string const &fieldName
  ) const
{
  for (size_t i = 0; i < source.fields.size(); i++) {
    if (source.fields[i].name == fieldName) return &source.fields[i];
  }

  return nullptr;
}

/* Return the configuration for the given row, ie. the Column from the
 * saved source if we have one, or the default config (for any undrawn
 * fields) */
conf::DashboardWidgetChart::Column
  TimeChartFunctionFieldsModel::findFieldConfiguration(int row) const
{
  std::string const fieldName(
    headerData(row, Qt::Vertical).toString().toStdString());

  conf::DashboardWidgetChart::Column const *conf =
    findFieldConfiguration(fieldName);

  if (conf) return *conf;

  return conf::DashboardWidgetChart::Column(
    fieldName, conf::DashboardWidgetChart::Column::Unused,
    0, 0x808080, 1.);
}

conf::DashboardWidgetChart::Column &
  TimeChartFunctionFieldsModel::findFieldConfiguration(int row)
{
  std::string const fieldName(
    headerData(row, Qt::Vertical).toString().toStdString());

  conf::DashboardWidgetChart::Column const *conf =
    findFieldConfiguration(fieldName);

  if (conf) {
    return const_cast<conf::DashboardWidgetChart::Column&>(*conf);
  } else {
    if (verbose)
      qDebug() << "model: adding a field";
    source.fields.emplace_back(
      fieldName, conf::DashboardWidgetChart::Column::Unused,
      0, 0x808080, 1.);
    return source.fields.back();
  }
}

QVariant TimeChartFunctionFieldsModel::data(
  QModelIndex const &index, int role) const
{
  if (role != Qt::EditRole && role != Qt::DisplayRole) return QVariant();

  int const row(index.row());
  if (row < 0 || row >= numericFields.count()) return QVariant();

  int const col(index.column());
  if (col < 0 || col >= NumColumns) return QVariant();

  conf::DashboardWidgetChart::Column conf(
    findFieldConfiguration(row));

  switch (static_cast<Columns>(col)) {
    case ColRepresentation:
      if (role == Qt::DisplayRole) {
        return conf::DashboardWidgetChart::Column::nameOfRepresentation(
          conf.representation);
      } else if (role == Qt::EditRole) {
        return conf.representation;
      } else {
        return QVariant();
      }

    case ColFactors:
      {
        QStringList fs;
        for (std::string const s : conf.factors) {
          fs += QString::fromStdString(s);
        }
        return fs;
      }

    case ColAxis:
      return conf.axisNum;

    case ColColor:
      return conf.color;

    case ColOpacity:
      return conf.opacity;

    case NumColumns:  // not a real column number
      assert(false);
  }

  return QVariant();
}

QVariant TimeChartFunctionFieldsModel::headerData(
  int section, Qt::Orientation orientation, int role) const
{
  if (role != Qt::EditRole && role != Qt::DisplayRole) return QVariant();

  if (orientation == Qt::Vertical) {
    if (section < 0 || section >= numericFields.count()) return QVariant();
    return numericFields[section];
  } else if (orientation == Qt::Horizontal) {
    if (section < 0 || section >= NumColumns) return QVariant();
    switch (static_cast<Columns>(section)) {
      case ColRepresentation:
        return QString(tr("draw"));
      case ColFactors:
        return QString(tr("factors"));
      case ColAxis:
        return QString(tr("axis"));
      case ColColor:
        return QString(tr("color"));
      case ColOpacity:
        return QString(tr("opacity"));
      case NumColumns:
        assert(false);
    }
  }

  return QVariant();
}

bool TimeChartFunctionFieldsModel::setData(
  QModelIndex const &index, QVariant const &value, int role)
{
  if (role != Qt::EditRole) return false;

  int const row(index.row());
  if (row < 0 || row >= numericFields.count()) return false;

  int const col(index.column());
  if (col < 0 || col >= NumColumns) return false;

  conf::DashboardWidgetChart::Column &conf(
    findFieldConfiguration(row));

  switch (static_cast<Columns>(col)) {
    case ColRepresentation:
      conf.representation = static_cast<conf::DashboardWidgetChart::Column::Representation>(value.toInt());
      break;

    case ColFactors:
      {
        conf.factors.clear();
        QStringList const l = value.toStringList();
        for (int i = 0; i < l.count(); i++) {
          conf.factors.push_back(l[i].toStdString());
        }
      }
      break;

    case ColAxis:
      conf.axisNum = value.toInt();
      break;

    case ColColor:
      conf.color = value.toInt();
      break;

    case ColOpacity:
      conf.opacity = value.toDouble();
      break;

    case NumColumns:
      assert(false);
  }

  static const QVector<int> editedRoles({ Qt::EditRole, Qt::DisplayRole });
  emit dataChanged(index, index, editedRoles);
  return true;
}

Qt::ItemFlags TimeChartFunctionFieldsModel::flags(
  QModelIndex const &) const
{
  return Qt::ItemIsEnabled | Qt::ItemIsEditable;
}

void TimeChartFunctionFieldsModel::resetInfo(std::string const &k, KValue const &)
{
  if (k == infoKey) {
    // TODO: recompute the numericFields and signal that all data is reset
    return;
  }
}

bool TimeChartFunctionFieldsModel::setValue(
  conf::DashboardWidgetChart::Source const &source_)
{
  if (verbose)
    qDebug() << "model: setValue with " << source_.fields.size() << "fields";

  /* The structure of the model (number of columns/rows) actually does not
   * change because it depends only on numericFields, which changes only
   * when the function itself changes. So there is no need to reset the model:
   */

  source = source_;
  // TODO: only for fields that really have changed:
  emit dataChanged(index(0, 0),
                   index(numericFields.size() - 1, NumColumns - 1));

  return true;
}
