#ifndef OPERATIONSVIEW_H_190507
#define OPERATIONSVIEW_H_190507
#include <QSplitter>
#include "OperationsModel.h"
#include "GraphViewSettings.h"

class OperationsView : public QSplitter
{
  Q_OBJECT

  OperationsModel *model;
  GraphViewSettings *settings;

public:
  OperationsView(QWidget *parent = nullptr);
  ~OperationsView();
};

#endif
