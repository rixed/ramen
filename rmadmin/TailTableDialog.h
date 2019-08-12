#ifndef TAILTABLEDIALOG_H_190810
#define TAILTABLEDIALOG_H_190810
#include <memory>
#include <QMainWindow>

class Function;

class TailTableDialog : public QMainWindow
{
  Q_OBJECT

  std::shared_ptr<Function> function;

public:
  TailTableDialog(std::shared_ptr<Function>, QWidget *parent = nullptr);
};

#endif
