#ifndef GRAPHVIEW_H_190508
#define GRAPHVIEW_H_190508
#include <QGraphicsView>
#include <QGraphicsScene>
#include "OperationsModel.h"

/*
 * The actual GraphView:
 */

class GraphView : public QGraphicsView
{
  Q_OBJECT

  QGraphicsScene scene;

  // Have to save that one because we cannot rely on QModelIndex to provide it:
  // Note: models are supposed to outlive the views, aren't they?
  OperationsModel const *model;

public:
  GraphView(QWidget *parent = nullptr);
  ~GraphView();
  void setModel(OperationsModel const *);
  QSize sizeHint() const override;

public slots:
  void collapse(QModelIndex const &index);
  void expand(QModelIndex const &index);
  void update(QModelIndex const &index);
  // to be connected to the model rowsAboutToBeInserted signal:
  // to be connected to the model rowsInserted signal:
  void insertRows(const QModelIndex &parent, int first, int last);
};

#endif
