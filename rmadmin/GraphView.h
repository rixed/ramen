#ifndef GRAPHVIEW_H_190508
#define GRAPHVIEW_H_190508
#include <unordered_map>
#include <QGraphicsScene>
#include <QGraphicsView>
#include <QTimer>
#include "GraphModel.h"

class GraphArrow;

/* Graph is layout on a fixed size grid, where functions occupy the cells.
 * Each cell dimension is functionGridWidth * functionGridHeight.
 * Then, each cell can contain either nothing, or a function, or a function
 * and a short program header, or a function and a short program header and
 * a short site header. The short header meaning: only the name of the
 * program or site.
 *
 * So we need a larger upper margin for all those headers.
 * Margin1 is the smaller (for sites), Margin2 the intermediary (for
 * programs) and Margin3 the larger (for functions).
 *
 * Then in between those Margin1 will flow all the arrows, so we want enough
 * place there.
 */

class GraphViewSettings;

struct HashStupidPairOfPointers {
  size_t operator()(const std::pair<GraphItem const *, GraphItem const *>& key) const {
    return (size_t)key.first + (size_t)key.second;
  }
};

class GraphView : public QGraphicsView
{
  Q_OBJECT

  QGraphicsScene scene;

  // Have to save that one because we cannot rely on QModelIndex to provide it:
  // Note: models are supposed to outlive the views, aren't they?
  GraphModel const *model;

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
  std::unordered_map<
    std::pair<GraphItem const *, GraphItem const *>,
    std::pair<GraphArrow *, bool>,
    HashStupidPairOfPointers
  > arrows;

  /* Used to trigger a layout computation after no functions were updated
   * for a short while: */
  QTimer layoutTimer;

  // Parameters we must share with the GraphItems:
  GraphViewSettings const *settings;

  qreal currentScale;
  qreal lastScale;

public:
  GraphView(GraphViewSettings const *, QWidget *parent = nullptr);
  void setModel(GraphModel const *);
  QSize sizeHint() const override;
protected:
  void keyPressEvent(QKeyEvent *) override;
  bool event(QEvent *) override;

protected slots:
  void updateArrows();

public slots:
  void zoom(qreal);
  void collapse(QModelIndex const &index);
  void expand(QModelIndex const &index);
  void select(QModelIndex const &index);
  // to be connected to the model rowsInserted signal:
  void insertRows(const QModelIndex &parent, int first, int last);
  // to be connected to the model signals of the same names:
  void relationAdded(FunctionItem const *, FunctionItem const *);
  void relationRemoved(FunctionItem const *, FunctionItem const *);
  void startLayout();
  void selectionChanged();

signals:
  void selected(QModelIndex const &);
};

#endif
