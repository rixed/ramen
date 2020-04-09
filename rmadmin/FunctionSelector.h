#ifndef FUNCTIONSELECTOR_H_191215
#define FUNCTIONSELECTOR_H_191215
/* A simple widget to pick a worker */
#include "TreeComboBox.h"

class FunctionItem;
class GraphModel;

class FunctionSelector : public TreeComboBox
{
  Q_OBJECT

  FunctionItem *previous;

public:
  FunctionSelector(GraphModel *model, QWidget *parent = nullptr);
  FunctionItem *getCurrent() const;

signals:
  void selectionChanged(FunctionItem *);

protected slots:
  /* Receives all selection changes from the QComboBox and emits
   * selectionChanged signals with either the function or nullptr: */
  void filterSelection();
  void resizeToContent();
};

#endif
