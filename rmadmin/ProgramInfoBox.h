#ifndef PROGRAMINFOBOX_H_190516
#define PROGRAMINFOBOX_H_190516
#include <string>
#include "AtomicForm.h"

class ProgramItem;
class QTableWidget;

class ProgramInfoBox : public AtomicForm
{
  Q_OBJECT

  ProgramItem const *p;
  std::string pref; // key prefix

  QTableWidget *paramTable;

public:
  ProgramInfoBox(ProgramItem const *, QWidget *parent = nullptr);

public slots:
  void setParam(conf::Key const &k, std::shared_ptr<conf::Value const>);
  void delParam(conf::Key const &k);
};

#endif
