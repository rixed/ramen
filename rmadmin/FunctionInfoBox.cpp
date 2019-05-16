#include <QLabel>
#include <QGridLayout>
#include "FunctionItem.h"
#include "FunctionInfoBox.h"

FunctionInfoBox::FunctionInfoBox(FunctionItem const *f_, QWidget *parent) :
  QWidget(parent), f(f_)
{
  QGridLayout *layout = new QGridLayout;
  setLayout(layout);
  // Title: name of the function
  QLabel *title = new QLabel(f->fqName());
  layout->addWidget(title, 0, 0, 1, 2, Qt::AlignHCenter);

  // some more infos...

  // Input type:
  layout->addWidget(new QLabel(tr("input type")), 1, 0);
  QLabel *inputTypeInfo = new QLabel("TODO");
  layout->addWidget(inputTypeInfo, 1, 1);

  // Output type:
  layout->addWidget(new QLabel(tr("output type")), 2, 0);
  QLabel *outputTypeInfo = new QLabel("TODO");
  layout->addWidget(outputTypeInfo, 2, 1);

  // TODO: Could as well display what's current;y displayed in the graph
}
