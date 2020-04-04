#include <cassert>
#include <string>
#include <QDebug>
#include "conf.h"
#include "dashboard/tools.h"
#include "misc.h"

#include "dashboard/DashboardTreeModel.h"

static bool const verbose(false);

DashboardTreeModel *DashboardTreeModel::globalDashboardTree;

DashboardTreeModel::DashboardTreeModel(QObject *parent)
  : ConfTreeModel(parent),
    addedScratchpad(false)
{
  addScratchpad();

  connect(kvs, &KVStore::keyChanged,
          this, &DashboardTreeModel::onChange);
}

void DashboardTreeModel::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        updateNames(change.key, change.kv);
        break;
      case KeyDeleted:
        deleteNames(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void DashboardTreeModel::updateNames(std::string const &key, KValue const &)
{
  addScratchpad();

  std::pair<QString const, std::string const> name_prefix(
    dashboardNameAndPrefOfKey(key));
  if (name_prefix.first.isEmpty()) return;

  if (verbose)
    qDebug() << "DashboardTreeModel: found" << name_prefix.first;

  QStringList names(name_prefix.first.split('/'));

  (void)findOrCreate(root, names, QString::fromStdString(name_prefix.second));
}

void DashboardTreeModel::deleteNames(std::string const &key, KValue const &)
{
  addScratchpad();

  QString const name(dashboardNameOfKey(key));
  if (name.isEmpty()) return;

  // TODO: actually delete? Or keep the names around for a bit?
}

void DashboardTreeModel::addScratchpad()
{
  if (addedScratchpad || !my_socket) return;

  /* We start with no scratchpad widgets, but we'd like a scratchpad entry
   * nonetheless so add it manually: */
  QStringList names({ "scratchpad" });
  std::string const prefix("clients/" + *my_socket + "/scratchpad");
  (void)findOrCreate(root, names, QString::fromStdString(prefix));
  addedScratchpad = true;
}
