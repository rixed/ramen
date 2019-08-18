#ifndef NAMESTREE_H_190816
#define NAMESTREE_H_190816
/* This object listen to all sites/.../worker and also to all sources/.../info
 * and build a tree with all user visible names as site/fq/field.
 * There is going to be a single global instance of this object.
 *
 * It is then possible to create a model based anywhere in that tree and down
 * to some given depth.
 * These models can then be used with QCompleter to autocomplete site names,
 * qualified functions, field names, whatever.
 * Note that those QCompleter must split at '/' (so we'll have to customize a
 * QCompleter).
 * So the right QCompleter can be obtained directly from the NamesTree
 */
#include <QAbstractItemModel>

class KVPair;
class SubTree;
class QStringList;

/*
 * The NamesTree is a model. A proxy could restrict it to some subtree (and,
 * optionally, to some types of name).
 */

class NamesTree : public QAbstractItemModel
{
  Q_OBJECT

  SubTree *root;

  SubTree *findOrCreate(SubTree *, QStringList &);

public:
  static NamesTree *globalNamesTree;

  NamesTree(QObject *parent = nullptr);
  ~NamesTree();

  // The QAbstractModel:
  QModelIndex index(int, int, QModelIndex const &) const;
  QModelIndex parent(QModelIndex const &) const;
  int rowCount(QModelIndex const &) const;
  int columnCount(QModelIndex const &) const;
  QVariant data(QModelIndex const &, int) const;

protected slots:
  void updateNames(KVPair const &kvp);
  void deleteNames(KVPair const &kvp);
};

/*
 * Now given any QAbstractItemModel, we can use that proxy to restrict it
 * to some subtree.
 */

#include <QPersistentModelIndex>

class NamesSubtree : public NamesTree
{
  Q_OBJECT

  QPersistentModelIndex newRoot;

public:
  NamesSubtree(QModelIndex newRoot_) : newRoot(newRoot_) {}

  QModelIndex index(int, int, QModelIndex const &) const;
  QModelIndex parent(QModelIndex const &) const;
};

/*
 * QCompleter for any NamesTree:
 */

#include <QCompleter>

class NamesCompleter : public QCompleter
{
  Q_OBJECT

  NamesTree const *model;

public:
  NamesCompleter(NamesTree *, QObject *parent = nullptr);

  QStringList splitPath(QString const &) const override;

  QString pathFromIndex(QModelIndex const &) const override;
};

#endif
