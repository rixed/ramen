#ifndef PROGRAMINFOBOX_H_190516
#define PROGRAMINFOBOX_H_190516
#include "AtomicForm.h"

class ProgramItem;
class KBool;
class KLineEdit;
class KLabel;

class ProgramInfoBox : public AtomicForm
{
  Q_OBJECT

  ProgramItem const *p;

  KBool *mustRun;
  KBool *debug;
  KLineEdit *reportPeriod;
  KLabel *binPath;
  KLabel *srcPath;

public:
  ProgramInfoBox(ProgramItem const *, QWidget *parent = nullptr);
};

#endif
