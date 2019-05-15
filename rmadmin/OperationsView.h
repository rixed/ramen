#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
#include <QSplitter>
#include <QRadioButton>
#include <QTreeView>
#include "GraphViewSettings.h"

class NarrowTreeView;
class GraphModel;

class OperationsView : public QSplitter
{
  Q_OBJECT

  GraphModel *graphModel;
  GraphViewSettings *settings;
  NarrowTreeView *treeView;
  bool allowReset;

  // Radio buttons for quickly set the desired Level Of Detail:
  QRadioButton *toSites, *toPrograms, *toFunctions;

public:
  OperationsView(QWidget *parent = nullptr);
  ~OperationsView();

public slots:
  void resetLOD(); // release all LOD radio buttons
  void setLOD(bool); // set a given LOD
};

#endif
