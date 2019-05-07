#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
#include <QTreeView>
#include <QWidget>

class OperationsView : public QTreeView
{
  Q_OBJECT

public:
  OperationsView(QWidget *parent = nullptr);
  ~OperationsView();
};

#endif
