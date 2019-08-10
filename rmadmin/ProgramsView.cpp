#include "TargetConfigEditor.h"
#include "ProgramsView.h"

ProgramsView::ProgramsView(QWidget *parent) :
  AtomicForm(parent)
{
  // TODO: Add a search box

  rcEditor = new TargetConfigEditor;
  rcEditor->setKey(conf::Key("target_config"));
  setCentralWidget(rcEditor);
  addWidget(rcEditor);
}
