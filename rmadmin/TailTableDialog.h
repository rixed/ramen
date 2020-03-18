#ifndef TAILTABLEDIALOG_H_190810
#define TAILTABLEDIALOG_H_190810
#include <memory>
#include <QMainWindow>

class TailModel;

class TailTableDialog : public QMainWindow
{
  Q_OBJECT

public:
  TailTableDialog(std::shared_ptr<TailModel>,
                  QWidget *parent = nullptr);
};

#endif
