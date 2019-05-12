#ifndef OPERATIONSMODEL_H_190507
#define OPERATIONSMODEL_H_190507
#include <vector>
#include <QVector>
#include <QPointF>
#include <QAbstractItemModel>
#include <QTimer>
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

class SiteItem;
class ProgramItem;
class FunctionItem;
class GraphViewSettings;

class OperationsModel : public QAbstractItemModel
{
  Q_OBJECT

  // So they can createIndex():
  friend class SiteItem;
  friend class ProgramItem;
  friend class FunctionItem;

  GraphViewSettings const *settings;
  unsigned paletteSize;

  void reorder();
  FunctionItem const *findWorker(std::shared_ptr<conf::Worker const>);
  void setFunctionParent(FunctionItem const *parent, FunctionItem *child, int idx);
  void delaySetFunctionParent(FunctionItem *child, int idx, std::shared_ptr<conf::Worker const>);
  void retrySetParents();
  void setFunctionProperty(FunctionItem *, QString const &p, std::shared_ptr<conf::Value const>);
  void setProgramProperty(ProgramItem *, QString const &p, std::shared_ptr<conf::Value const>);
  void setSiteProperty(SiteItem *, QString const &p, std::shared_ptr<conf::Value const>);

public:
  std::vector<SiteItem *> sites;

  OperationsModel(GraphViewSettings const *, QObject *parent = nullptr);

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
  void relationAdded(FunctionItem const *parent, FunctionItem const *child) const;
  void relationRemoved(FunctionItem const *parent, FunctionItem const *child) const;
};

#endif
