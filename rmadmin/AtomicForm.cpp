#include <iostream>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include "AtomicForm.h"
#include "conf.h"

AtomicForm::AtomicForm(QString const &title, QWidget *parent) :
  QGroupBox(title, parent),
  widgets(),
  state(AtomicForm::ReadOnly)
{
  widgets.reserve(5);

  groupLayout = new QVBoxLayout(this);
  this->setLayout(groupLayout);

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

  // The button bar
  QWidget *buttonBar = new QWidget;
  buttonsLayout = new QHBoxLayout(buttonBar);
  buttonBar->setLayout(buttonsLayout);
  editButton = new QPushButton(tr("&edit"));
  buttonsLayout->addWidget(editButton);
  connect(editButton, &QPushButton::clicked, this, &AtomicForm::wantEdit);
  cancelButton = new QPushButton(tr("&cancel"));
  buttonsLayout->addWidget(cancelButton);
  connect(cancelButton, &QPushButton::clicked, this, &AtomicForm::wantCancel);
  cancelButton->setEnabled(false);
  submitButton = new QPushButton(tr("&submit"));
  buttonsLayout->addWidget(submitButton);
  connect(submitButton, &QPushButton::clicked, this, &AtomicForm::wantSubmit);
  submitButton->setEnabled(false);
  groupLayout->addWidget(buttonBar);

  /* Also prepare the confirmation dialog: */
  confirmationDialog = new QMessageBox(this);
  confirmationDialog->setText("Some values have been modified.");
  confirmationDialog->setInformativeText("Are you sure you want to cancel?");
  confirmationDialog->setStandardButtons(QMessageBox::Yes | QMessageBox::No);
  confirmationDialog->setDefaultButton(QMessageBox::No);
}

AtomicForm::~AtomicForm()
{
  // TODO: unlock whatever widget is locked
}

void AtomicForm::setCentralWidget(QWidget *w)
{
  QLayoutItem *previous =
    groupLayout->replaceWidget(centralWidget, w, Qt::FindDirectChildrenOnly);
  assert(previous);
  delete previous;
}

void AtomicForm::addWidget(AtomicWidget *aw)
{
  aw->setEnabled(false);
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[aw->key];
  conf::kvs_lock.unlock_shared();
  widgets.push_back(aw);
  connect(&kv, &KValue::valueLocked, this, &AtomicForm::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &AtomicForm::unlockValue);
}

void AtomicForm::lockAll()
{
  std::cout << "lock all!" << std::endl;
  state = Locking;
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
  state = Unlocking;
  for (AtomicWidget *aw : widgets) {
    aw->setValue(aw->key, aw->initValue);
    conf::askUnlock(aw->key);
  }
}

void AtomicForm::wantCancel()
{
  if (someEdited()) {
    if (QMessageBox::Yes == confirmationDialog->exec()) {
      doCancel();
    }
  } else {
    doCancel();
  }
}

void AtomicForm::doSubmit()
{
  state = Unlocking;
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
  submitButton->setEnabled(enabled);
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
