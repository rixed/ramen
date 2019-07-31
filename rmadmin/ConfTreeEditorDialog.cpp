#include <QLabel>
#include <QDialogButtonBox>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QLineEdit>
#include "conf.h"
#include "KValue.h"
#include "AtomicWidget.h"
#include "ConfTreeEditorDialog.h"

ConfTreeEditorDialog::ConfTreeEditorDialog(conf::Key const &key_, KValue const *kv, QWidget *parent) :
  QDialog(parent),
  key(key_)
{
  QLabel *keyLabel = new QLabel(tr("Key:"));
  /* Before long we will want to edit key names: */
  QLineEdit *keyEditor = new QLineEdit;
  keyEditor->setText(QString::fromStdString(key.s));
  keyEditor->setEnabled(false);

  editor = kv->val->editorWidget(key);
  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);

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
  conf::askLock(key);

  /* Now the layout: */
  QHBoxLayout *keyLayout = new QHBoxLayout;
  keyLayout->addWidget(keyLabel);
  keyLayout->addWidget(keyEditor);
  QVBoxLayout *mainLayout = new QVBoxLayout;
  mainLayout->addLayout(keyLayout);
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
  conf::askSet(key, v);
  conf::askUnlock(key);
  emit QDialog::accept();
}

void ConfTreeEditorDialog::cancel()
{
  conf::askUnlock(key);
}
