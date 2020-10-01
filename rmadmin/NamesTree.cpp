#include <cassert>
#include <QAbstractItemModel>
#include <QDebug>
#include <QtGlobal>
#include <QVariant>
#include "conf.h"
#include "ConfSubTree.h"
#include "confValue.h"
#include "confWorkerRole.h"
#include "misc.h"
#include "RamenType.h"

#include "NamesTree.h"

static bool const verbose(false);

NamesTree *NamesTree::globalNamesTree;
NamesTree *NamesTree::globalNamesTreeAnySites;

/*
 * The NamesTree itself:
 *
 * Also a model.
 * We then have a proxy than select only a subtree.
 */

NamesTree::NamesTree(bool withSites_, QObject *parent)
  : ConfTreeModel(parent), withSites(withSites_)
{
  connect(kvs, &KVStore::keyChanged,
          this, &NamesTree::onChange);
}

void NamesTree::onChange(QList<ConfChange> const &changes)
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

std::pair<std::string, std::string> NamesTree::pathOfIndex(
  QModelIndex const &index) const
{
  std::pair<std::string, std::string> ret { std::string(), std::string() };

  if (! index.isValid()) return ret;

  ConfSubTree *s = static_cast<ConfSubTree *>(index.internalPointer());

  if (!s->isTerm() && verbose)
    qWarning() << s->name << "is not a terminal yet is on term position";

  while (s != root) {
    std::string *n = s->isTerm() ? &ret.second : &ret.first;
    if (! n->empty()) n->insert(0, "/");
    n->insert(0, s->name.toStdString());
    s = s->parent;
  }

  return ret;
}

static bool isAWorker(std::string const &key)
{
  return startsWith(key, "sites/") && endsWith(key, "/worker");
}

void NamesTree::updateNames(std::string const &key, KValue const &kv)
{
  if (! isAWorker(key)) return;

  std::shared_ptr<conf::Worker const> worker =
    std::dynamic_pointer_cast<conf::Worker const>(kv.val);
  if (! worker) {
    qCritical() << "Not a worker!?";
    return;
  }

  if (worker->role->isTopHalf) return;

  /* Get the site name, program name list and function name: */

  size_t const prefix_len = sizeof "sites/" - 1;
  size_t const workers_len = sizeof "/workers/" - 1;
  size_t const suffix_len = sizeof "/worker" - 1;
  size_t const min_names = sizeof "s/p/f" - 1;

  if (key.length() < prefix_len + workers_len + suffix_len + min_names) {
invalid_key:
    qCritical() << "Invalid worker key:" << QString::fromStdString(key);
    return;
  }

  size_t const end = key.length() - suffix_len;

  size_t i = key.find('/', prefix_len);
  if (i >= end) goto invalid_key;
  QString const site =
    QString::fromStdString(key.substr(prefix_len, i - prefix_len));

  // Skip "/workers/"
  i = key.find('/', i+1);
  if (i >= end) goto invalid_key;

  // Locate the function name at the end:
  size_t j = key.rfind('/', end - 1);
  if (j <= i) goto invalid_key;

  // Everything in between in the program name:
  std::string const programName(key.substr(i + 1, j - i - 1));
  std::string const srcPath = srcPathFromProgramName(programName);
  QString const programs = QString::fromStdString(programName);
  QStringList const program =
    programs.split('/', QString::SkipEmptyParts);
  QString const function =
    QString::fromStdString(key.substr(j + 1, end - j - 1));

  if (verbose)
    qDebug() << "NamesTree: found" << site << "/ "
              << programs << "/" << function;

  QStringList names(QStringList(site) << program << function);
  if (! withSites) names.removeFirst();

  ConfSubTree *func = findOrCreate(root, names, QString());

  /* Now get the field names */

  /* In theory keys are synced in the same order as created so we should
   * received the source info before any worker using it during a sync: */
  std::string infoKey =
    "sources/" + srcPath + "/info";

  std::shared_ptr<conf::SourceInfo const> sourceInfos;

  kvs->lock.lock_shared();
  auto it = kvs->map.find(infoKey);
  if (it != kvs->map.end()) {
    sourceInfos = std::dynamic_pointer_cast<conf::SourceInfo const>(it->second.val);
    if (! sourceInfos)
      qCritical() << "NamesTree: Not a SourceInfo!?";
  }
  kvs->lock.unlock_shared();

  if (! sourceInfos) {
    if (verbose)
      qDebug() << "NamesTree: No source info yet for"
               << QString::fromStdString(infoKey);
    return;
  }

  if (sourceInfos->isError()) {
    if (verbose)
      qDebug() << "NamesTree:" << QString::fromStdString(infoKey)
               << "not compiled yet";
    return;
  }

  /* In the sourceInfos all functions of that program could be found, but for
   * simplicity let's add only the one we came for: */
  for (auto &info : sourceInfos->infos) {
    if (info->name != function) continue;
    std::shared_ptr<DessserValueType> s(info->outType->vtyp);
    /* FIXME: Each column could have subcolumns and all should be inserted
     * hierarchically. */
    /* Some type info for the field (stored in the model as UserType+... would
     * come handy, for instance to determine if a field is numeric, or a
     * factor, etc */
    for (int c = 0; c < s->numColumns(); c ++) {
      QStringList names(s->columnName(c));
      (void)findOrCreate(func, names, names.last());
    }
    break;
  }

/*  if (verbose) {
    qDebug() << "NamesTree: Current names-tree:";
    dump();
  }*/
}

void NamesTree::deleteNames(std::string const &key, KValue const &)
{
  if (! isAWorker(key)) return;

  // TODO: actually delete? Or keep the names around for a bit?
}

/*
 * If we are already in the subtree, then to ensure we do not leave it the only
 * function that require adapting is parent() (so we do not leave the subtree
 * up the root). index() also need to be adapted to enumeration starts from the
 * subtree.
 */

NamesSubtree::NamesSubtree(NamesTree const &namesTree, QModelIndex const &newRoot_)
  : NamesTree(namesTree.withSites),
    newRoot(newRoot_)
{
  /* This new NamesTree will update itself from conftree signals, but let's
   * copy the passed namesTree to not start from empty: */
  delete root;
  root = new ConfSubTree(*(namesTree.root), nullptr);
}

QModelIndex NamesSubtree::index(int row, int column, QModelIndex const &parent) const
{
  if (! parent.isValid()) {
    return NamesTree::index(row, column, newRoot);
  }
  return NamesTree::index(row, column, parent);
}

QModelIndex NamesSubtree::parent(QModelIndex const &index) const
{
  if (index == newRoot) return QModelIndex();

  return NamesTree::parent(index);
}

int NamesSubtree::rowCount(QModelIndex const &index) const
{
  return NamesTree::rowCount(index.isValid() ? index : QModelIndex(newRoot));
}

int NamesSubtree::columnCount(QModelIndex const &index) const
{
  return NamesTree::columnCount(index.isValid() ? index : QModelIndex(newRoot));
}

QVariant NamesSubtree::data(QModelIndex const &index, int role) const
{
  return NamesTree::data(index.isValid() ? index : QModelIndex(newRoot), role);
}

/*
 * Just teach QCompleter how to convert a string to/from a path:
 */

NamesCompleter::NamesCompleter(
  NamesTree *model, QObject *parent, QModelIndex const &newRoot_)
  : QCompleter(model, parent),
    newRoot(newRoot_)
{
  if (verbose)
    qDebug() << "NamesCompleter: root label:"
             << (newRoot.isValid() ? model->data(newRoot, Qt::DisplayRole) : "");

  setCompletionRole(Qt::DisplayRole);
  setModelSorting(QCompleter::CaseSensitivelySortedModel);
}

QStringList NamesCompleter::splitPath(QString const &path_) const
{
  QString path(path_);
  QAbstractItemModel const *mod = model();

  for (QModelIndex i = newRoot; i.isValid(); i = mod->parent(i)) {
    if (! path.isEmpty()) path.prepend('/');
    path.prepend(mod->data(i, Qt::DisplayRole).toString());
  }

  /* Would be nice to SkipEmptyParts, but the last one must not be skipped, or
   * the completer would not jump to the next level of the tree. */
  return path.split('/');
}

QString NamesCompleter::pathFromIndex(QModelIndex const &index) const
{
  if (!index.isValid()) {
    // possible when newRoot is the root
    assert(!newRoot.isValid());
    return QString(); // Path from model root
  }

  ConfSubTree *tree = static_cast<ConfSubTree *>(index.internalPointer());

  QString ret(tree->name);

  ConfSubTree *root =
    newRoot.isValid() ?
      static_cast<ConfSubTree *>(QModelIndex(newRoot).internalPointer()) : nullptr;
  if (verbose)
    qDebug() << "NamesCompleter: root@" << root << ":" << root->name;

  while (tree->parent && tree->parent->parent && tree->parent != root) {
    tree = tree->parent;
    if (verbose)
      qDebug() << "NamesCompleter: tree@" << tree;
    ret.prepend('/');
    ret.prepend(tree->name);
  }

  return ret;
}
