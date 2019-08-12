#include <iostream>
#include <ctime>
#include <cassert>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QPushButton>
#include <QFormLayout>
#include "RCEntryEditor.h"
#include "conf.h"
#include "confValue.h"
#include "RamenValue.h"
#include "NewProgramDialog.h"

static conf::Key rc_key("target_config");

NewProgramDialog::NewProgramDialog(QString const &sourceName, QWidget *parent) :
  QDialog(parent),
  mustSave(false)
{
  bool const sourceEditable = sourceName.isEmpty();
  editor = new RCEntryEditor(sourceEditable);
  editor->setSourceName(sourceName);

  /* Cannot use a QDialogButtonBox because the form therein is leaking
   * all its keypress events and a mere enter would accept without validation
   * and a single press on escape would close the dialog and forget all user
   * edits with no second though.
   * Notice that even a button called "Cancel" may be set as the default,
   * but at least when not using QDialogButtonBox it is possible to remove
   * that default. */
  okButton = new QPushButton(tr("Submit"));
  okButton->setAutoDefault(false);
  okButton->setDefault(false);
  okButton->setEnabled(false);
  QPushButton *cancelButton = new QPushButton(tr("Cancel"));
  cancelButton->setAutoDefault(false);
  cancelButton->setDefault(false);
  QHBoxLayout *buttonBox = new QHBoxLayout;
  buttonBox->addStretch();
  buttonBox->addWidget(cancelButton);
  buttonBox->addWidget(okButton);

  connect(editor, &RCEntryEditor::inputChanged,
          this, &NewProgramDialog::validate);
  connect(okButton, &QPushButton::clicked,
          this, &NewProgramDialog::createProgram);
  connect(cancelButton, &QPushButton::clicked,
          this, &QDialog::reject);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(editor);
  layout->addLayout(buttonBox);
  setLayout(layout);

  // Listen for all locks on the RC:
  std::cout << "Listening to RC locks..." << std::endl;
  conf::kvs_lock.lock_shared();
  KValue &rc_value = conf::kvs[rc_key].kv;
  connect(&rc_value, &KValue::valueLocked,
          this, &NewProgramDialog::mayWriteRC);
  conf::kvs_lock.unlock_shared();

  setWindowTitle(tr("Start New Program"));
}

void NewProgramDialog::createProgram()
{
  if (! editor->isValid()) return;

  mustSave = true;

  /* This is where having a single config entry for the whole RC is
   * inconvenient: We have to lock it, modify it, and unlock it.
   * Had we one entry per program we could simply use NewKey.
   * Here instead we write only if/when we obtain the lock. */
  conf::kvs_lock.lock_shared();
  KValue &rc_value = conf::kvs[rc_key].kv;
  conf::kvs_lock.unlock_shared();
  if (rc_value.isMine()) {
    appendEntry();
  } else {
    conf::askLock(rc_key);
  }
}

void NewProgramDialog::mayWriteRC(conf::Key const &, QString const &uid, double)
{
  if (mustSave && uid == my_uid) appendEntry(); // else wait longer...
}

void NewProgramDialog::appendEntry()
{
  if (! mustSave) return;

  std::cout << "Appending a new RC entry" << std::endl;
  conf::RCEntry *rce = editor->getValue();

  conf::kvs_lock.lock_shared();
  KValue &rc_value = conf::kvs[rc_key].kv;
  conf::kvs_lock.unlock_shared();

  std::shared_ptr<conf::TargetConfig> rc =
    rc_value.isSet() ?
      // No support for improper type here:
      std::dynamic_pointer_cast<conf::TargetConfig>(rc_value.val) :
      std::make_shared<conf::TargetConfig>();

  if (rc) {
    rc->addEntry(rce);
    std::cout << "Added entry with " << rce->params.size() << " params" << std::endl;
    askSet(rc_key, std::static_pointer_cast<conf::Value const>(rc));
  } else {
    std::cerr << "Invalid type for the TargetConfig!?" << std::endl;
  }

  mustSave = false;
  askUnlock(rc_key);
  /* Maybe reset the editor? */
  emit QDialog::accept();
}

void NewProgramDialog::validate()
{
  okButton->setEnabled(editor->isValid());
}
