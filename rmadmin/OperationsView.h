#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
#include <QSplitter>
#include "GraphViewSettings.h"

class NarrowTreeView;
class GraphModel;
class TailModel;
class QTabWidget;
class QRadioButton;
class FunctionItem;

class OperationsView : public QSplitter
{
  Q_OBJECT

  GraphModel *graphModel;
  TailModel *tailModel;
  GraphViewSettings *settings;
  NarrowTreeView *treeView;
  QTabWidget *tailTabs;
  bool allowReset;

  // Radio buttons for quickly set the desired Level Of Detail:
  QRadioButton *toSites, *toPrograms, *toFunctions;

public:
  OperationsView(QWidget *parent = nullptr);
  ~OperationsView();

public slots:
  void resetLOD(); // release all LOD radio buttons
  void setLOD(bool); // set a given LOD
  void addTail(FunctionItem const *);
};

#endif
