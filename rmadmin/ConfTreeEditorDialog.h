#ifndef CONFTREEEDITORDIALOG_H_190729
#define CONFTREEEDITORDIALOG_H_190729
#include <QDialog>
#include "confKey.h"

class AtomicWidget;
class KValue;

class ConfTreeEditorDialog : public QDialog
{
  Q_OBJECT

  AtomicWidget *editor;
  conf::Key const key;
  bool can_write;

public:
  ConfTreeEditorDialog(conf::Key const &k, KValue const *kv, QWidget *parent = nullptr);

private slots:
  void save();
  void cancel();
};

#endif
