#include <cassert>
#include <QDebug>
#include <QtGlobal>
#include <QVariant>
#include "ConfSubTree.h"

#include "ConfTreeModel.h"

static bool const verbose(false);

ConfTreeModel::ConfTreeModel(QObject *parent)
  : QAbstractItemModel(parent)
{
  root = new ConfSubTree(QString(), nullptr, QString());
}

ConfTreeModel::~ConfTreeModel()
{
  delete root;
}

void ConfTreeModel::dump() const
{
  root->dump();
}

QModelIndex ConfTreeModel::index(
  int row, int column, QModelIndex const &parent) const
{
  ConfSubTree *parentTree =
    parent.isValid() ?
      static_cast<ConfSubTree *>(parent.internalPointer()) :
      root;

  if (verbose && parent.isValid())
    qDebug() << "ConfTreeModel: index(" << row << ") of "
             << data(parent, Qt::DisplayRole);

  if (row < 0 || row >= parentTree->count()) return QModelIndex();

  return createIndex(row, column, parentTree->child(row));
}

QModelIndex ConfTreeModel::parent(QModelIndex const &index) const
{
  assert(index.isValid());

  ConfSubTree *tree = static_cast<ConfSubTree *>(index.internalPointer());
  if (tree->parent == root) return QModelIndex();
  return createIndex(tree->parent->childNum(tree), 0, tree->parent);
}

int ConfTreeModel::rowCount(QModelIndex const &index) const
{
  if (! index.isValid()) return root->count();
  ConfSubTree *tree = static_cast<ConfSubTree *>(index.internalPointer());
  return tree->count();
}

int ConfTreeModel::columnCount(QModelIndex const &) const
{
  return 1;
}

QVariant ConfTreeModel::data(QModelIndex const &index, int role) const
{
  assert(index.isValid());
  ConfSubTree *tree = static_cast<ConfSubTree *>(index.internalPointer());

  switch (role) {
    case Qt::DisplayRole:
      return tree->name;

    case Qt::UserRole:  // Can be requested by QComboBox.currentData()
      if (tree->termValue.isEmpty()) {
        return QVariant();
      } else {
        return tree->termValue;
      }

    default:
      return QVariant();
  }
}

QModelIndex ConfTreeModel::find(std::string const &path) const
{
  QStringList names(QString::fromStdString(path).
                    split('/', QString::SkipEmptyParts));

  QModelIndex idx;
  ConfSubTree const *parent = root;

  do {
    if (names.count() == 0) return idx;

    QString const &name = names.takeFirst();

    for (int i = 0; i < parent->count(); i ++) {
      ConfSubTree const *c = parent->child(i);
      int const cmp(name.compare(c->name));
      if (cmp > 0) continue;
      if (cmp < 0) {
        if (verbose)
          qDebug() << "ConfTreeModel: Cannot find" << QString::fromStdString(path);
        return QModelIndex();
      }
      parent = c;
      idx = index(i, 0, idx);
      break;
    }

  } while (true);
}

ConfSubTree *ConfTreeModel::findOrCreate(
  ConfSubTree *parent, QStringList &names, QString const termValue)
{
  assert(parent == root || parent->parent != nullptr);

  if (names.count() == 0) return parent;

  QString const &name = names.takeFirst();

  /* Look for it in the (ordered) list of subtrees: */
  int i;
  for (i = 0; i < parent->count(); i ++) {
    ConfSubTree *c = parent->child(i);
    int const cmp(name.compare(c->name));
    if (cmp > 0) continue;
    if (cmp == 0) {
      if (verbose)
        qDebug() << "ConfTreeModel:" << name << "already in the tree";
      return findOrCreate(c, names, termValue);
    }
    break;
  }

  // Insert the new name at position i:
  if (verbose)
    qDebug() << "ConfTreeModel: inserting" << name
             << "with termValue" << termValue;

  QModelIndex parentIndex =
    parent == root ?
      QModelIndex() :
      createIndex(parent->parent->childNum(parent), 0, parent);
  beginInsertRows(parentIndex, i, i);
  ConfSubTree *n = parent->insertAt(i, name, termValue);
  endInsertRows();
  return findOrCreate(n, names, termValue);
}

bool ConfTreeModel::isTerm(QModelIndex const &index) const
{
  if (! index.isValid()) return false;

  ConfSubTree *s = static_cast<ConfSubTree *>(index.internalPointer());

  return s->isTerm();
}
