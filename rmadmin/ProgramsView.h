#ifndef PROGRAMSVIEW_H_190611
#define PROGRAMSVIEW_H_190611
#include "AtomicForm.h"

class TargetConfigEditor;

class ProgramsView : public AtomicForm
{
  Q_OBJECT

  TargetConfigEditor *rcEditor;

public:
  ProgramsView(QWidget *parent = nullptr);
};

#endif
