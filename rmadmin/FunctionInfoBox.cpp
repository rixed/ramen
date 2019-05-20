#include <QLabel>
#include <QFormLayout>
#include "FunctionItem.h"
#include "KLabel.h"
#include "FunctionInfoBox.h"

FunctionInfoBox::FunctionInfoBox(FunctionItem const *f_, QWidget *parent) :
  QWidget(parent),
  f(f_),
  pref("programs/" + f->treeParent->name.toStdString() + "/functions/" +
       f->name.toStdString() + "/")
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  KLabel *retention = new KLabel(pref + "retention", this);
  layout->addRow(tr("Retention"), retention);

  KLabel *doc = new KLabel(pref + "doc", this);
  layout->addRow(tr("Documentation"), doc);

  KLabel *isLazy = new KLabel(pref + "is_lazy", this);
  layout->addRow(tr("Lazy?"), isLazy);

  KLabel *operation = new KLabel(pref + "operation", this);
  layout->addRow(tr("Operation"), operation);

  KLabel *inType = new KLabel(pref + "type/in", this);
  layout->addRow(tr("Input Type"), inType);

  KLabel *outType = new KLabel(pref + "type/out", this);
  layout->addRow(tr("Output Type"), outType);

  KLabel *signature = new KLabel(pref + "signature", this);
  layout->addRow(tr("Signature"), signature);

  KLabel *mergeInputs = new KLabel(pref + "merge_inputs", this);
  layout->addRow(tr("Distinct Inputs"), mergeInputs);
}
