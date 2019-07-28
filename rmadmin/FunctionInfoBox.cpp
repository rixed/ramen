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
  pref("programs/" + f->treeParent->name.toStdString() + "/functions/" +
       f->name.toStdString() + "/")
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  KLabel *retention = new KLabel(pref + "retention", false, this);
  layout->addRow(tr("Retention"), retention);

  KLabel *doc = new KLabel(pref + "doc", false, this);
  layout->addRow(tr("Documentation"), doc);

  KLabel *isLazy = new KLabel(pref + "is_lazy", false, this);
  layout->addRow(tr("Lazy?"), isLazy);

  KLabel *operation = new KLabel(pref + "operation", false, this);
  layout->addRow(tr("Operation"), operation);

  KLabel *inType = new KLabel(pref + "type/in", false, this);
  layout->addRow(tr("Input Type"), inType);

  KLabel *outType = new KLabel(pref + "type/out", false, this);
  layout->addRow(tr("Output Type"), outType);

  KLabel *signature = new KLabel(pref + "signature", false, this);
  layout->addRow(tr("Signature"), signature);

  KLabel *mergeInputs = new KLabel(pref + "merge_inputs", false, this);
  layout->addRow(tr("Distinct Inputs"), mergeInputs);
}
