#include <cassert>
#include <QDebug>
#include <QModelIndex>
#include <QVariant>
#include "conf.h"
#include "GraphModel.h"
#include "RamenType.h"
#include "Resources.h"

#include "chart/TimeChartFunctionFieldsModel.h"

static bool const verbose(false);

TimeChartFunctionFieldsModel::TimeChartFunctionFieldsModel(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  QObject *parent)
  : QAbstractTableModel(parent),
    source(site, program, function)
{
  connect(GraphModel::globalGraphModel, &GraphModel::workerChanged,
          this, &TimeChartFunctionFieldsModel::checkSource);
}

int TimeChartFunctionFieldsModel::rowCount(QModelIndex const &) const
{
  return numericFields.count();
}

int TimeChartFunctionFieldsModel::columnCount(QModelIndex const &) const
{
  return NumColumns;
}

conf::DashWidgetChart::Column const *
  TimeChartFunctionFieldsModel::findFieldConfiguration(
    std::string const &fieldName
  ) const
{
  for (conf::DashWidgetChart::Column const &field : source.fields) {
    if (field.name == fieldName) return &field;
  }

  return nullptr;
}

/* Return the configuration for the given row, ie. the Column from the
 * saved source if we have one, or the default config (for any undrawn
 * fields) */
conf::DashWidgetChart::Column
  TimeChartFunctionFieldsModel::findFieldConfiguration(int row) const
{
  std::string const fieldName(
    headerData(row, Qt::Vertical).toString().toStdString());

  conf::DashWidgetChart::Column const *conf =
    findFieldConfiguration(fieldName);

  if (conf) return *conf;

  return conf::DashWidgetChart::Column(
    source.program, source.function, fieldName);
}

conf::DashWidgetChart::Column &
  TimeChartFunctionFieldsModel::findFieldConfiguration(int row)
{
  std::string const fieldName(
    headerData(row, Qt::Vertical).toString().toStdString());

  conf::DashWidgetChart::Column const *conf =
    findFieldConfiguration(fieldName);

  if (conf) {
    return const_cast<conf::DashWidgetChart::Column&>(*conf);
  } else {
    if (verbose)
      qDebug() << "model: adding a field";
    source.fields.emplace_back(source.program, source.function, fieldName);
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

  conf::DashWidgetChart::Column conf(
    findFieldConfiguration(row));

  switch (static_cast<Columns>(col)) {
    case ColRepresentation:
      return conf.representation;

    case ColFactors:
      if (role == Qt::DisplayRole) {
        if (conf.factors.empty())
          return Resources::get()->emptyIcon;
        else
          return Resources::get()->factorsIcon;
    } else if (role == Qt::EditRole) {
      QStringList lst;
      for (std::string const &f : conf.factors) {
        lst += QString::fromStdString(f);
      }
      return lst;
    }
    break;

    case ColAxis:
      return conf.axisNum;

    case ColColor:
      return conf.color;

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

  conf::DashWidgetChart::Column &conf(
    findFieldConfiguration(row));

  switch (static_cast<Columns>(col)) {
    case ColRepresentation:
      conf.representation =
        static_cast<conf::DashWidgetChart::Column::Representation>(
          value.toInt());
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
      conf.color = value.value<QColor>();
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

bool TimeChartFunctionFieldsModel::setValue(
  conf::DashWidgetChart::Source const &source_)
{
  if (verbose)
    qDebug() << "model: setValue with " << source_.fields.size() << "fields";

  /* The structure of the model can change entirely from the previous one.
   * Start by getting a list of all numeric fields (similar to NamesTree): */
  std::string const infoKey {
    "sources/" + srcPathFromProgramName(source_.program) + "/info" };

  std::shared_ptr<conf::SourceInfo const> sourceInfos;
  {
    kvs->lock.lock_shared();
    auto it = kvs->map.find(infoKey);
    if (it != kvs->map.end()) {
      sourceInfos =
        std::dynamic_pointer_cast<conf::SourceInfo const>(it->second.val);
      if (! sourceInfos)
        qCritical() << "TimeChartFunctionFieldsModel: Not a SourceInfo!?";
    }
    kvs->lock.unlock_shared();
  }

  if (! sourceInfos) {
    qWarning() << "TimeChartFunctionFieldsModel: Cannot get field of"
               << QString::fromStdString(infoKey);
    return false;
  }

  if (sourceInfos->isError()) {
    qWarning() << "TimeChartFunctionFieldsModel:"
               << QString::fromStdString(infoKey) << "is not compiled yet";
    return false;
  }

  beginResetModel();
  numericFields.clear();
  factors.clear();

  QString function_(QString::fromStdString(source_.function));
  for (auto &info : sourceInfos->infos) {
    if (info->name != function_) continue;
    std::shared_ptr<DessserValueType const> s(info->outType->vtyp);
    for (int c = 0; c < s->numColumns(); c++) {
      QString const columnName = s->columnName(c);
      std::shared_ptr<RamenType const> t(s->columnType(c));
      if (t->vtyp->isNumeric()) numericFields += columnName;
      if (info->factors.contains(columnName)) factors += columnName;
    }
    break;
  }

  if (verbose)
    qDebug() << "TimeChartFunctionFieldsModel: found these numeric fields:"
             << numericFields;

  source = source_;

  /* Filter out fields that are not numeric (or does not exist any longer) */
  {
    auto it = source.fields.begin();
    auto end = source.fields.end();
    while (it != end) {
      QString const name { QString::fromStdString(it->name) };
      if (!numericFields.contains(name)) {
        qWarning() << "configured field" << name
                   << "does not exist or is not numeric";
        source.fields.erase(it++);
      } else {
        ++it;
      }
    }
  }

  endResetModel();
  return true;
}

/* Called whenever a worker has changed */
void TimeChartFunctionFieldsModel::checkSource(
  QString const &oldSign, QString const &newSign)
{
  if (oldSign == newSign) return;

  setValue(source);
}

bool TimeChartFunctionFieldsModel::hasSelection() const
{
  for (conf::DashWidgetChart::Column const &c : source.fields) {
    if (c.representation != conf::DashWidgetChart::Column::Unused)
      return true;
  }

  return false;
}
