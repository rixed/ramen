#include <QLabel>
#include <QDialogButtonBox>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QFormLayout>
#include <QLineEdit>
#include "misc.h"
#include "conf.h"
#include "KValue.h"
#include "confValue.h"
#include "AtomicWidget.h"
#include "ConfTreeEditorDialog.h"

ConfTreeEditorDialog::ConfTreeEditorDialog(
  std::string const &key_, QWidget *parent) :
  QDialog(parent),
  key(key_)
{
  /* Locate the value in the kvs: */
  KValue const *kv = nullptr;
  kvs->lock.lock_shared();
  auto it = kvs->map.find(key);
  if (it != kvs->map.end()) kv = &it->second;
  kvs->lock.unlock_shared();
  if (! kv) {
    assert(!"TODO: display a QLabel(error) instead");
  }
  can_write = kv->can_write;

  /* The header: */
  QFormLayout *headerLayout = new QFormLayout;
  QLabel *keyName = new QLabel(QString::fromStdString(key));
  keyName->setWordWrap(true);
  headerLayout->addRow(tr("Key:"), keyName);
  QLabel *setter = new QLabel(kv->uid);
  headerLayout->addRow(tr("Last Modified By:"), setter);
  QLabel *mtime = new QLabel(stringOfDate(kv->mtime));
  headerLayout->addRow(tr("Last Modified At:"), mtime);
  if (kv->isLocked()) {
    QLabel *locker = new QLabel(*kv->owner);
    headerLayout->addRow(tr("Locked By:"), locker);
    QLabel *expiry = new QLabel(stringOfDate(kv->expiry));
    headerLayout->addRow(tr("Expiry:"), expiry);
  }

  editor = kv->val->editorWidget(key);
  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(
      can_write ? QDialogButtonBox::Ok | QDialogButtonBox::Cancel :
                  QDialogButtonBox::Close);  // Note: Close will reject

  if (can_write)
    connect(buttonBox, &QDialogButtonBox::accepted,
            this, &ConfTreeEditorDialog::save);
  /* Connect first the cancel button to the normal reject signal, and
   * then this signal to our cancel slot, so that cancel is also called
   * when user presses the escape key: */
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &QDialog::reject);
  connect(this, &QDialog::rejected,
          this, &ConfTreeEditorDialog::cancel);

  /* The editor will start in read-only mode (unless we already own the
   * value). Reception of the lock ack from the confserver will turn it
   * into read-write mode: */
  if (can_write) askLock(key);

  /* Now the layout: */
  QVBoxLayout *mainLayout = new QVBoxLayout;
  QFrame* header = new QFrame;
  header->setFrameShape(QFrame::Panel);
  header->setFrameShadow(QFrame::Raised);
  header->setLayout(headerLayout);
  mainLayout->addWidget(header);
  mainLayout->addWidget(editor);
  mainLayout->addWidget(buttonBox);
  setLayout(mainLayout);

  /* Now that the layout is known, set window decorations, sizes etc: */
  setWindowTitle(tr("Value Editor"));
  setSizeGripEnabled(true); // editors of various types vary largely in size
}

void ConfTreeEditorDialog::save()
{
  std::shared_ptr<conf::Value const> v(editor->getValue());
  if (v) askSet(key, v); // read-only editors return no value
  if (can_write) askUnlock(key);
  emit QDialog::accept();
}

void ConfTreeEditorDialog::cancel()
{
  if (can_write) askUnlock(key);
}
