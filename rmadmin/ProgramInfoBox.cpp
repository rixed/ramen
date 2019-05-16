#include <QLabel>
#include <QGridLayout>
#include "ProgramItem.h"
#include "ProgramInfoBox.h"

ProgramInfoBox::ProgramInfoBox(ProgramItem const *p_, QWidget *parent) :
  QWidget(parent), p(p_)
{
  QGridLayout *layout = new QGridLayout;
  setLayout(layout);
  // Title: name of the program (no mention of the site, the info we display
  // are specific to the program and does not depend on which site it's
  // running in)
  QLabel *title = new QLabel(p->name);
  layout->addWidget(title, 0, 0, 1, 2, Qt::AlignHCenter);

  // TODO: some of this is editable: make this an AtomicForm

  layout->addWidget(new QLabel(tr("Must run")), 1, 0);
  QLabel *inputTypeInfo = new QLabel("TODO");
  layout->addWidget(inputTypeInfo, 1, 1);
}
