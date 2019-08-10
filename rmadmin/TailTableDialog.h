#ifndef TAILTABLEDIALOG_H_190810
#define TAILTABLEDIALOG_H_190810
#include <QMainWindow>

class FunctionItem;

class TailTableDialog : public QMainWindow
{
  Q_OBJECT

  FunctionItem *function;

public:
  TailTableDialog(FunctionItem *, QWidget *parent = nullptr);
};

#endif
