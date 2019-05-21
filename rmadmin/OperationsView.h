#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
#include <QSplitter>
#include "GraphViewSettings.h"

class NarrowTreeView;
class GraphModel;
class QTabWidget;
class QRadioButton;
class FunctionItem;
class ProgramItem;

class OperationsView : public QSplitter
{
  Q_OBJECT

  GraphModel *graphModel;
  GraphViewSettings *settings;
  NarrowTreeView *treeView;
  QTabWidget *infoTabs;
  QTabWidget *dataTabs;
  bool allowReset;

  // Radio buttons for quickly set the desired Level Of Detail:
  QRadioButton *toSites, *toPrograms, *toFunctions;

public:
  OperationsView(QWidget *parent = nullptr);
  ~OperationsView();

signals:
  void functionSelected(FunctionItem *);
  void programSelected(ProgramItem *);

public slots:
  void resetLOD(); // release all LOD radio buttons
  void setLOD(bool); // set a given LOD
  void addTail(FunctionItem *);
  void remTail(int index);
  void addSource(ProgramItem const *);
  void addProgInfo(ProgramItem const *);
  void addFuncInfo(FunctionItem const *);
  // Will retrieve the function and emit functionSelected()
  void selectItem(QModelIndex const &); // the QModelIndex from the graphModel
  void closeInfo(int);
  void closeData(int);
};

#endif
