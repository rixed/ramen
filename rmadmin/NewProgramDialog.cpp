#include <iostream>
#include <ctime>
#include <cassert>
#include <QDialogButtonBox>
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
  editor = new RCEntryEditor(sourceEditable, sourceName);

  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);

  connect(buttonBox, &QDialogButtonBox::accepted,
          this, &NewProgramDialog::createProgram);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &QDialog::reject);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(editor);
  layout->addWidget(buttonBox);
  setLayout(layout);

  // Listen for all locks on the RC:
  std::cout << "Listening to RC locks..." << std::endl;
  conf::kvs_lock.lock_shared();
  KValue &rc_value = conf::kvs[rc_key];
  connect(&rc_value, &KValue::valueLocked,
          this, &NewProgramDialog::mayWriteRC);
  conf::kvs_lock.unlock_shared();

  setWindowTitle(tr("Start New Program"));
}

void NewProgramDialog::createProgram()
{
  mustSave = true;

  /* This is where having a single config entry for the whole RC is
   * inconvenient: We have to lock it, modify it, and unlock it.
   * Had we one entry per program we could simply use NewKey.
   * Here instead we write only if/when we obtain the lock. */
  conf::kvs_lock.lock_shared();
  KValue rc_value = conf::kvs[rc_key];
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
  KValue rc_value = conf::kvs[rc_key];
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
