#ifndef CONFTREEEDITORDIALOG_H_190729
#define CONFTREEEDITORDIALOG_H_190729
#include <QDialog>

class AtomicWidget;

class ConfTreeEditorDialog : public QDialog
{
  Q_OBJECT

  AtomicWidget *editor;
  std::string const key;
  bool can_write;

public:
  ConfTreeEditorDialog(std::string const &k, QWidget *parent = nullptr);

private slots:
  void save();
  void cancel();
};

#endif
