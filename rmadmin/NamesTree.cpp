#include <iostream>
#include <cassert>
#include <QAbstractItemModel>
#include <QVariant>
#include "conf.h"
#include "misc.h"
#include "KVPair.h"
#include "confValue.h"
#include "confWorkerRole.h"
#include "RamenType.h"
#include "NamesTree.h"

static bool const verbose = false;

NamesTree *NamesTree::globalNamesTree;

/*
 * What's used to store the names.
 */

class SubTree
{
  std::vector<SubTree *> children;

public:
  /* Cannot be const because SubTrees are constructed inplace, but
   * that's the idea: */
  QString name;

  SubTree *parent;

  bool isField;

  SubTree(QString const &name_, SubTree *parent_, bool isField_) :
    name(name_), parent(parent_), isField(isField_)
  {
    if (verbose)
      std::cout << "NamesTree: Creating SubTree(name=" << name.toStdString()
                << ", parent=" << (parent ? parent->name.toStdString() : "none")
                << ")" << std::endl;
    children.reserve(10);
  }

  ~SubTree()
  {
    for (SubTree *c : children) delete c;
  }

  int count() const
  {
    return children.size();
  }

  SubTree *child(unsigned pos)
  {
    assert(pos < children.size());
    return children[pos];
  }

  int childNum(SubTree const *child) const
  {
    if (verbose)
      std::cout << "childNum(" << child->name.toStdString() << ")"
                << " of " << name.toStdString() << std::endl;

    for (int c = 0; c < (int)children.size(); c ++) {
      if (children[c]->name == child->name &&
          children[c] != child)
        std::cerr << "not unique child address for " << child->name.toStdString() << "; " << children[c] << " vs " << child << std::endl;
      if (children[c] == child) return c;
    }
    assert(!"Not a child");
    return -1;
  }

  void dump(std::string const &indent = "") const
  {
    for (SubTree *c : children) {
      std::cout << indent << c->name.toStdString() << " (parent="
                << c->parent->name.toStdString() << ")\n";
      c->dump(indent + "  ");
    }
  }

  SubTree *insertAt(unsigned pos, QString const &name, bool isField)
  {
    SubTree *s = new SubTree(name, this, isField);
    children.insert(children.begin() + pos, s);
    return s;
  }
};

/*
 * The NamesTree itself:
 *
 * Also a model.
 * We then have a proxy than select only a subtree.
 */

NamesTree::NamesTree(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new SubTree(QString(), nullptr, false);

  connect(&kvs, &KVStore::valueCreated,
          this, &NamesTree::updateNames);
  connect(&kvs, &KVStore::valueChanged,
          this, &NamesTree::updateNames);
  connect(&kvs, &KVStore::valueDeleted,
          this, &NamesTree::deleteNames);
}

NamesTree::~NamesTree()
{
  delete root;
}

static bool isAWorker(std::string const &key)
{
  return startsWith(key, "sites/") && endsWith(key, "/worker");
}

// Empties [names]
SubTree *NamesTree::findOrCreate(
  SubTree *parent, QStringList &names, bool isField)
{
  if (names.count() == 0) return parent;

  QString const &name = names.takeFirst();

  /* Look for it in the (ordered) list of subtrees: */
  int i;
  for (i = 0; i < parent->count(); i ++) {
    SubTree *c = parent->child(i);
    if (name > c->name) continue;
    if (name == c->name) {
      if (verbose)
        std::cout << "NamesTree: " << name.toStdString()
                  << " already in the tree" << std::endl;
      return findOrCreate(c, names, isField);
    }
    break;
  }

  // Insert the new name at position i:
  QModelIndex parentIndex =
    parent == root ?
      QModelIndex() :
      createIndex(parent->parent->childNum(parent), 0, parent);
  beginInsertRows(parentIndex, i, i);
  SubTree *n = parent->insertAt(i, name, isField);
  endInsertRows();
  return findOrCreate(n, names, isField);
}

QModelIndex NamesTree::find(std::string const &path) const
{
  QStringList names(QString::fromStdString(path).
                    split('/', QString::SkipEmptyParts));

  QModelIndex idx;
  SubTree *parent = root;

  do {
    if (names.count() == 0) return idx;

    QString const &name = names.takeFirst();

    for (int i = 0; i < parent->count(); i ++) {
      SubTree *c = parent->child(i);
      if (name > c->name) continue;
      if (name < c->name) {
        if (verbose)
          std::cout << "NamesTree: Cannot find " << path << std::endl;
        return QModelIndex();
      }
      parent = c;
      idx = index(i, 0, idx);
      break;
    }

  } while (true);
}

bool NamesTree::isField(QModelIndex const &index) const
{
  if (! index.isValid()) return false;

  SubTree *s = static_cast<SubTree *>(index.internalPointer());
  return s->isField;
}

std::pair<std::string, std::string> NamesTree::pathOfIndex(
  QModelIndex const &index) const
{
  std::pair<std::string, std::string> ret { std::string(), std::string() };

  if (! index.isValid()) return ret;

  SubTree *s = static_cast<SubTree *>(index.internalPointer());
  while (s != root) {
    std::string *n = s->isField ? &ret.second : &ret.first;
    if (! n->empty()) n->insert(0, "/");
    n->insert(0, s->name.toStdString());
    s = s->parent;
  }

  return ret;
}

void NamesTree::updateNames(KVPair const &kvp)
{
  if (! isAWorker(kvp.first)) return;

  std::shared_ptr<conf::Worker const> worker =
    std::dynamic_pointer_cast<conf::Worker const>(kvp.second.val);
  if (! worker) {
    std::cerr << "Not a worker!?" << std::endl;
    return;
  }

  if (worker->role->isTopHalf) return;

  /* Get the site name, program name list and function name: */

  size_t const prefix_len = sizeof "sites/" - 1;
  size_t const workers_len = sizeof "/workers/" - 1;
  size_t const suffix_len = sizeof "/worker" - 1;
  size_t const min_names = sizeof "s/p/f" - 1;

  if (kvp.first.length() < prefix_len + workers_len + suffix_len + min_names) {
invalid_key:
    std::cerr << "Invalid worker key: " << kvp.first << std::endl;
    return;
  }

  size_t const end = kvp.first.length() - suffix_len;

  size_t i = kvp.first.find('/', prefix_len);
  if (i >= end) goto invalid_key;
  QString const site =
    QString::fromStdString(kvp.first.substr(prefix_len, i - prefix_len));

  // Skip "/workers/"
  i = kvp.first.find('/', i+1);
  if (i >= end) goto invalid_key;

  // Locate the function name at the end:
  size_t j = kvp.first.rfind('/', end - 1);
  if (j <= i) goto invalid_key;

  // Everything in between in the program name:
  QString const programs =
    QString::fromStdString(kvp.first.substr(i + 1, j - i - 1));
  QStringList const program =
    programs.split('/', QString::SkipEmptyParts);
  QString const function =
    QString::fromStdString(kvp.first.substr(j + 1, end - j - 1));

  if (verbose)
    std::cout << "NamesTree: found " << site.toStdString() << " / "
              << programs.toStdString() << " / " << function.toStdString()
              << std::endl;

  QStringList names(QStringList(site) << program << function);

  SubTree *func = findOrCreate(root, names, false);

  /* Now get the field names */

  std::string infoKey =
    "sources/" + worker->srcPath.toStdString() + "/info";

  std::shared_ptr<conf::SourceInfo const> sourceInfos;

  kvs.lock.lock_shared();
  auto it = kvs.map.find(infoKey);
  if (it == kvs.map.end()) {
    if (verbose)
      std::cout << "NamesTree: No source info yet for " << infoKey << std::endl;
  } else {
    sourceInfos = std::dynamic_pointer_cast<conf::SourceInfo const>(it->second.val);
    if (! sourceInfos)
      std::cerr << "NamesTree: Not a SourceInfo!?" << std::endl;
  }
  kvs.lock.unlock_shared();

  if (! sourceInfos) return;
  if (sourceInfos->isError()) {
    if (verbose)
      std::cout << "NamesTree: " << infoKey << " not compiled yet" << std::endl;
    return;
  }

  /* In the sourceInfos all functions of that program could be found, but for
   * simplicity let's add only the one we came for: */
  for (auto it : sourceInfos->infos) {
    if (it.name != function) continue;
    std::shared_ptr<RamenTypeStructure> s(it.out_type->structure);
    /* FIXME: Each column should have subcolumns and all should be inserted
     * hierarchically. */
    for (unsigned c = 0; c < s->numColumns(); c ++) {
      QStringList names(s->columnName(c));
      (void)findOrCreate(func, names, true);
    }
    break;
  }

  if (verbose) {
    std::cout << "NamesTree: Current names-tree:" << std::endl;
    root->dump();
  }
}

void NamesTree::deleteNames(KVPair const &kvp)
{
  if (! isAWorker(kvp.first)) return;

  // TODO: actually delete? Or keep the names around for a bit?
}

/*
 * The model for names
 */

QModelIndex NamesTree::index(int row, int column, QModelIndex const &parent) const
{
  SubTree *parentTree =
    parent.isValid() ?
      static_cast<SubTree *>(parent.internalPointer()) :
      root;

  if (row >= parentTree->count()) return QModelIndex();

  return createIndex(row, column, parentTree->child(row));
}

QModelIndex NamesTree::parent(QModelIndex const &index) const
{
  assert(index.isValid());

  SubTree *tree = static_cast<SubTree *>(index.internalPointer());
  if (tree->parent == root) return QModelIndex();
  return createIndex(tree->parent->childNum(tree), 0, tree->parent);
}

int NamesTree::rowCount(QModelIndex const &index) const
{
  if (! index.isValid()) return root->count();
  SubTree *tree = static_cast<SubTree *>(index.internalPointer());
  return tree->count();
 }

int NamesTree::columnCount(QModelIndex const &) const
{
  return 1;
}

QVariant NamesTree::data(QModelIndex const &index, int role) const
{
  assert(index.isValid());
  if (role != Qt::DisplayRole) return QVariant();
  SubTree *tree = static_cast<SubTree *>(index.internalPointer());
  return tree->name;
}

/*
 * If we are already in the subtree, then to ensure we do not leave it the only
 * function that require adapting is parent() (so we do not leave the subtree
 * up the root). index() also need to be adapted to enumeration starts from the
 * subtree.
 */

QModelIndex NamesSubtree::index(int row, int column, QModelIndex const &parent) const
{
  if (! parent.isValid())
    return NamesTree::index(row, column, newRoot);
  return NamesTree::index(row, column, parent);
}

QModelIndex NamesSubtree::parent(QModelIndex const &index) const
{
  if (index == newRoot) return QModelIndex();
  return NamesTree::parent(index);
}

/*
 * Just teach QCompleter how to to convert a string to/from a path:
 */

NamesCompleter::NamesCompleter(NamesTree *model_, QObject *parent) :
  QCompleter(model_, parent), model(model_)
{
  setCompletionRole(Qt::DisplayRole);
  setModelSorting(QCompleter::CaseSensitivelySortedModel);
}

QStringList NamesCompleter::splitPath(QString const &path) const
{
  /* Would be nice to SkipEmptyParts, but the last one must not be skipped, or
   * the completer would not jump to the next level of the tree. */
  return path.split('/');
}

QString NamesCompleter::pathFromIndex(QModelIndex const &index) const
{
  assert(index.isValid());

  SubTree *tree = static_cast<SubTree *>(index.internalPointer());

  QString ret(tree->name);

  while (tree->parent && tree->parent->parent) {
    tree = tree->parent;
    ret.prepend('/');
    ret.prepend(tree->name);
  }

  return ret;
}
