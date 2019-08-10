#include <iostream>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include "AtomicForm.h"
#include "conf.h"

AtomicForm::AtomicForm(QWidget *parent) :
  QGroupBox(parent),
  widgets()
{
  widgets.reserve(5);

  groupLayout = new QVBoxLayout(this);
  groupLayout->setContentsMargins(QMargins());
  setLayout(groupLayout);

  /* So we want 3 vertical areas:
   * - the "central widget" (to be set later)
   * - an error area
   * - the button bar "edit" / "cancel"+"submit"
   */
  // The central widget
  centralWidget = new QWidget;
  groupLayout->addWidget(centralWidget, 1);

  // The errors area
  errorArea = new QWidget;
  groupLayout->addWidget(errorArea);

  /* The button bar
   * According to MacOS UI guidelines, actions are supposed to be at right
   * and cancellation/going back on the left.
   * We keep the rightmost position for the "submit" action and keep
   * "delete" before it, in fear that users might go for the right-most
   * button out of habit. */
  buttonsLayout = new QHBoxLayout;
  editButton = new QPushButton(tr("&edit"));
  buttonsLayout->addWidget(editButton);
  connect(editButton, &QPushButton::clicked, this, &AtomicForm::wantEdit);

  cancelButton = new QPushButton(tr("&cancel"));
  buttonsLayout->addWidget(cancelButton);
  connect(cancelButton, &QPushButton::clicked, this, &AtomicForm::wantCancel);
  cancelButton->setEnabled(false);

  deleteButton = new QPushButton(tr("&delete"));
  buttonsLayout->addWidget(deleteButton);
  connect(deleteButton, &QPushButton::clicked, this, &AtomicForm::wantDelete);
  deleteButton->setEnabled(false);
  deleteButton->hide(); // until a deletable widget is added

  submitButton = new QPushButton(tr("&submit"));
  buttonsLayout->addWidget(submitButton);
  connect(submitButton, &QPushButton::clicked, this, &AtomicForm::wantSubmit);
  submitButton->setEnabled(false);

  groupLayout->addLayout(buttonsLayout);

  /* Also prepare the confirmation dialogs: */
  confirmCancelDialog = new QMessageBox(this);
  confirmCancelDialog->setText("Some values have been modified.");
  confirmCancelDialog->setInformativeText("Are you sure you want to cancel?");
  confirmCancelDialog->setStandardButtons(QMessageBox::Yes | QMessageBox::No);
  confirmCancelDialog->setDefaultButton(QMessageBox::No);
  confirmCancelDialog->setIcon(QMessageBox::Question);

  confirmDeleteDialog = new QMessageBox(this);
  confirmDeleteDialog->setText("Are you sure you want to delete this?");
  confirmDeleteDialog->setStandardButtons(QMessageBox::Yes | QMessageBox::Cancel);
  confirmDeleteDialog->setDefaultButton(QMessageBox::Cancel);
  confirmDeleteDialog->setIcon(QMessageBox::Warning);
}

AtomicForm::~AtomicForm()
{
  // Unlock everything that's locked:
  for (conf::Key const &k : locked) {
    std::cout << "Unlocking " << k << std::endl;
    askUnlock(k);
  }
}

void AtomicForm::setCentralWidget(QWidget *w)
{
  QLayoutItem *previous =
    groupLayout->replaceWidget(centralWidget, w, Qt::FindDirectChildrenOnly);
  assert(previous);
  delete previous;
  /* Do not automatically add to the widget as the form central widget
   * need not be an AtomicWidget. */
}

void AtomicForm::addWidget(AtomicWidget *aw, bool deletable)
{
  widgets.push_back(aw);
  if (deletable) {
    deletables.insert(aw);
    deleteButton->show();
  }

  connect(aw, &AtomicWidget::keyChanged,
          this, &AtomicForm::changeKey);

  // If key is already set, start from it:
  if (aw->key != conf::Key::null)
    changeKey(conf::Key::null, aw->key);
}

void AtomicForm::changeKey(conf::Key const &oldKey, conf::Key const &newKey)
{
  /* This is broken, as it will disconnect _all_ connection from that kv to us,
   * even if we have several widgets using the same key.
   * TODO: save the connect handlers and destroy them specifically.
   * We do not care for now as when we do have several widgets with the same
   * key then they will change their keys together. */
  conf::kvs_lock.lock_shared();
  if (oldKey != conf::Key::null) {
    disconnect(&conf::kvs[oldKey], 0, this, 0);
  }

  if (newKey != conf::Key::null) {
    KValue *kv = &conf::kvs[newKey];
    connect(kv, &KValue::valueLocked, this, &AtomicForm::lockValue);
    connect(kv, &KValue::valueUnlocked, this, &AtomicForm::unlockValue);
    if (kv->isLocked()) lockValue(newKey, *kv->owner);
  }
  conf::kvs_lock.unlock_shared();
}

void AtomicForm::wantEdit()
{
  // Lock all widgets that are not locked already:
  for (AtomicWidget const *aw : widgets) {
    if (locked.find(aw->key) == locked.end()) {
      conf::askLock(aw->key);
    }
  }
}

bool AtomicForm::someEdited()
{
  for (AtomicWidget const *aw : widgets) {
    std::shared_ptr<conf::Value const> v(aw->getValue());
    if (! v) return false;
    if (! aw->initValue) {
      std::cout << "Value of " << aw->key << " has been set to "
                << *v << std::endl;
      return true;
    }
    if (*aw->initValue != *v) {
      std::cout << "Value of " << aw->key << " has changed from "
                << *aw->initValue << " to " << *v << std::endl;
      return true;
    }
  }
  return false;
}

void AtomicForm::doCancel()
{
  for (AtomicWidget *aw : widgets) {
    aw->setValue(aw->key, aw->initValue);
    conf::askUnlock(aw->key);
  }
}

void AtomicForm::wantCancel()
{
  if (someEdited()) {
    if (QMessageBox::Yes == confirmCancelDialog->exec()) {
      doCancel();
    }
  } else {
    doCancel();
  }
}

void AtomicForm::wantDelete()
{
  if (deletables.empty()) return;

  QString info(tr("Those keys will be lost forever:\n"));
  for (AtomicWidget *aw : deletables) {
    info.append(QString::fromStdString(aw->key.s));
    info.append("\n");
  }
  confirmDeleteDialog->setInformativeText(info);

  if (QMessageBox::Yes == confirmDeleteDialog->exec()) {
    for (AtomicWidget *aw : deletables) {
      conf::askDel(aw->key);
    }
  }
}

void AtomicForm::doSubmit()
{
  for (AtomicWidget *aw : widgets) {
    std::shared_ptr<conf::Value const> v(aw->getValue());
    if (v && (! aw->initValue || *v != *aw->initValue))
      conf::askSet(aw->key, v);
    conf::askUnlock(aw->key);
  }
}

void AtomicForm::wantSubmit()
{
  if (someEdited()) {
    doSubmit();
  } else {
    std::cout << "Cancelling rather, as no edition was done." << std::endl;
    doCancel();
  }
}

void AtomicForm::setEnabled(bool enabled)
{
  // An enabled form is a form that's editable:
  editButton->setEnabled(! enabled);
  cancelButton->setEnabled(enabled);
  deleteButton->setEnabled(enabled);
  submitButton->setEnabled(enabled);

  emit changeEnabled(enabled);
}

void AtomicForm::lockValue(conf::Key const &k, QString const &u)
{
  bool const is_me = my_uid && *my_uid == u;

  std::cout << "locked key " << k << " to user " << u.toStdString()
            << " (I am " << my_uid->toStdString()
            << (is_me ? ", that's me!)" : ", not me)") << std::endl;
  if (is_me) {
    locked.insert(k);
  } else {
    locked.erase(k);
  }
  if (locked.size() >= widgets.size()) setEnabled(true);
}

void AtomicForm::unlockValue(conf::Key const &k)
{
  locked.erase(k);
  if (locked.size() <= widgets.size()) setEnabled(false);
}
