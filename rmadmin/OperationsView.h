#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
/* The OperationsView displays the GraphView and some controls. */
#include <QSplitter>
#include "GraphViewSettings.h"

class FunctionItem;
class GraphModel;
class NarrowTreeView;
class ProgramItem;
class QTabWidget;
class QRadioButton;

class OperationsView : public QSplitter
{
  Q_OBJECT

  NarrowTreeView *treeView;
  bool allowReset;

  // Radio buttons for quickly set the desired Level Of Detail:
  QRadioButton *toSites, *toPrograms, *toFunctions;

public:
  OperationsView(GraphModel *, QWidget *parent = nullptr);

signals:
  void functionSelected(FunctionItem *);
  void programSelected(ProgramItem *);

public slots:
  void resetLOD(); // release all LOD radio buttons
  void setLOD(bool); // set a given LOD
  void showSource(ProgramItem const *);
  void showFuncInfo(FunctionItem const *);
  // Will retrieve the function and emit functionSelected()
  void selectItem(QModelIndex const &); // the QModelIndex from the graphModel
};

#endif
