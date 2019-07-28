#include "TargetConfigEditor.h"
#include "ProgramsView.h"

ProgramsView::ProgramsView(QWidget *parent) :
  AtomicForm("Running Programs", parent)
{
  // Add a search box

  rcEditor = new TargetConfigEditor(conf::Key("target_config"));
  setCentralWidget(rcEditor);
  addWidget(rcEditor);
}
