#ifndef GRAPHVIEW_H_190508
#define GRAPHVIEW_H_190508
#include <unordered_map>
#include <QGraphicsView>
#include <QGraphicsScene>
#include "OperationsModel.h"

class GraphArrow;

/*
 * The actual GraphView:
 */

struct HashStupidPairOfPointers {
  size_t operator()(const std::pair<OperationsItem const *, OperationsItem const *>& key) const {
    return (size_t)key.first + (size_t)key.second;
  }
};

class GraphView : public QGraphicsView
{
  Q_OBJECT

  QGraphicsScene scene;

  // Have to save that one because we cannot rely on QModelIndex to provide it:
  // Note: models are supposed to outlive the views, aren't they?
  OperationsModel const *model;

  /* Relationships management:
   *
   * We store all current relationships and we recompute the whole set of
   * arrows each time we record a change in relationship or anything is
   * collapsed/expanded. These changes seldom happen anyway, but at the very
   * start (solvable via a short timeout to delay reconstruction of the
   * GraphView after a change). */
  std::unordered_multimap<FunctionItem const *, FunctionItem const *> relations;

  /* The current set of arrows, indexed by src+dst.
   * The bool is for tagging while updating. */
  std::unordered_map<std::pair<OperationsItem const *, OperationsItem const *>, std::pair<GraphArrow *, bool>, HashStupidPairOfPointers> arrows;

  void updateArrows();

  /* Used to trigger a layout computation after no functions were updated
   * for a short while: */
  QTimer layoutTimer;

public:
  GraphView(QWidget *parent = nullptr);
  ~GraphView();
  void setModel(OperationsModel const *);
  QSize sizeHint() const override;

public slots:
  void collapse(QModelIndex const &index);
  void expand(QModelIndex const &index);
  // to be connected to the model rowsInserted signal:
  void insertRows(const QModelIndex &parent, int first, int last);
  // to be connected to the model signals of the same names:
  void relationAdded(FunctionItem const *, FunctionItem const *);
  void relationRemoved(FunctionItem const *, FunctionItem const *);
  void startLayout();
};

#endif
