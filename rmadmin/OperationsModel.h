#ifndef OPERATIONSMODEL_H_190507
#define OPERATIONSMODEL_H_190507
#include <vector>
#include <QVector>
#include <QPointF>
#include <QAbstractItemModel>
#include "confKey.h"
#include "KValue.h"
#include "OperationsItem.h"

/* The tree is 3 layers deep:
 *
 * - site, with a name and a few other properties like number of workers and
 *   an is_master flag;
 * - programs, with again a name etc;
 * - function, with name etc.
 *
 * The tree model corresponds exactly to this structure, and is populated
 * using callbacks from the conf synchroniser.
 *
 * The GraphView uses the same data model. So the model must also be able
 * to return another set of parents (graph ancestors). The graphView slots
 * will be connected to the TreeView signals so that collapse/expand/select
 * are propagated. But as the GraphView does just the displaying (not the
 * graph layout) then the data model most hold the positions itself (the
 * positions are part of the data model, can be edited/saved by users...)
 * Meaning we need yet another member: [position] and another signal:
 * [positionChanged].
 */

class OperationsModel : public QAbstractItemModel
{
  Q_OBJECT

  friend class SiteItem;
  friend class ProgramItem;
  friend class FunctionItem;

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

signals:
  void positionChanged(QModelIndex const &index) const;
};

std::ostream &operator<<(std::ostream &, SiteItem const &);
std::ostream &operator<<(std::ostream &, ProgramItem const &);
std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
