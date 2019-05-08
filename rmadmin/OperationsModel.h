#ifndef OPERATIONSMODEL_H_190507
#define OPERATIONSMODEL_H_190507
#include <vector>
#include <QAbstractItemModel>
#include "confKey.h"
#include "KValue.h"

/* The tree is 3 layers deep:
 *
 * - site, with a name and a few other properties like number of workers and
 *   an is_master flag;
 * - programs, with again a name etc;
 * - function, with name etc.
 *
 * The tree model correspond exactly to this structure, and is populated
 * using callbacks from the conf synchroniser.
 */

class OperationsItem
{
  /* We store a pointer to the parents, because no item is ever reparented.
   * When a parent is deleted, it deletes recursively all its children. */
public:
  OperationsItem *parent;
  int row;
  OperationsItem(OperationsItem *parent_) : parent(parent_), row(0) {}
  virtual ~OperationsItem() = 0;
  virtual QVariant data(int) const = 0;
  // Reorder the children after some has been added/removed
  virtual void reorder() = 0;
};

class SiteItem;
class ProgramItem;

class FunctionItem : public OperationsItem
{
public:
  QVariant name;
  FunctionItem(OperationsItem *parent, QString name);
  ~FunctionItem();
  QVariant data(int) const;
  void reorder();
};

class ProgramItem : public OperationsItem
{
public:
  QVariant name;
  // As we are going to point to item from their children we do not want them
  // to move in memory, so let's use a vector of pointers:
  std::vector<FunctionItem *> functions;
  ProgramItem(OperationsItem *parent, QString name);
  ~ProgramItem();
  QVariant data(int) const;
  void reorder();
};

class SiteItem : public OperationsItem
{
public:
  QVariant name;
  std::vector<ProgramItem *> programs;
  SiteItem(OperationsItem *parent, QString name);
  ~SiteItem();
  QVariant data(int) const;
  void reorder();
};

class OperationsModel : public QAbstractItemModel
{
  Q_OBJECT

  std::vector<SiteItem *> sites;
  void reorder();

  /* To create models that populate incrementally, you can reimplement
   * fetchMore() and canFetchMore(). If the reimplementation of fetchMore()
   * adds rows to the model, beginInsertRows() and endInsertRows() must be
   * called */

public:
  OperationsModel(QObject *parent = nullptr);

  /* When subclassing QAbstractItemModel, at the very least you must implement
   * index(), parent(), rowCount(), columnCount(), and data(). These functions
   * are used in all read-only models, and form the basis of editable models.
   */
  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

public slots:
  void keyCreated(conf::Key const &, std::shared_ptr<conf::Value const>);
  void keyChanged(conf::Key const &, std::shared_ptr<conf::Value const>);
};

std::ostream &operator<<(std::ostream &, SiteItem const &);
std::ostream &operator<<(std::ostream &, ProgramItem const &);
std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
