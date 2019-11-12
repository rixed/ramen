#ifndef OPERATIONSWIN_H_191111
#define OPERATIONSWIN_H_191111
/* A window displaying the OperationsView widget. */
#include "SavedWindow.h"

class QWidget;

class OperationsWin : public SavedWindow
{
  Q_OBJECT

public:
  explicit OperationsWin(QWidget *parent = nullptr);
};

#endif
