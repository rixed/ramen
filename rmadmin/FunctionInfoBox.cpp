#include <QLabel>
#include <QFormLayout>
#include "FunctionItem.h"
#include "KLabel.h"
#include "FunctionInfoBox.h"

/* FIXME: Broken as the programs/.* keys does not exist any longer.
 * Use the source info instead. */

FunctionInfoBox::FunctionInfoBox(FunctionItem const *f_, QWidget *parent) :
  QWidget(parent),
  f(f_),
  pref("programs/" + f->treeParent->shared->name.toStdString() + "/functions/" +
       f->shared->name.toStdString() + "/")
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  KLabel *retention = new KLabel(this);
  retention->setKey(pref + "retention");
  layout->addRow(tr("Retention"), retention);

  KLabel *doc = new KLabel(this);
  doc->setKey(pref + "doc");
  layout->addRow(tr("Documentation"), doc);

  KLabel *isLazy = new KLabel(this);
  isLazy->setKey(pref + "is_lazy");
  layout->addRow(tr("Lazy?"), isLazy);

  KLabel *operation = new KLabel(this);
  operation->setKey(pref + "operation");
  layout->addRow(tr("Operation"), operation);

  KLabel *inType = new KLabel(this);
  inType->setKey(pref + "type/in");
  layout->addRow(tr("Input Type"), inType);

  KLabel *outType = new KLabel(this);
  outType->setKey(pref + "type/out");
  layout->addRow(tr("Output Type"), outType);

  KLabel *signature = new KLabel(this);
  signature->setKey(pref + "signature");
  layout->addRow(tr("Signature"), signature);

  KLabel *mergeInputs = new KLabel(this);
  mergeInputs->setKey(pref + "merge_inputs");
  layout->addRow(tr("Distinct Inputs"), mergeInputs);
}
