#include <iostream>
#include <QHBoxLayout>
#include "AtomicForm.h"
#include "conf.h"

AtomicForm::AtomicForm(QString const &title, QWidget *parent) :
  QGroupBox(title, parent),
  widgets(),
  state(AtomicForm::ReadOnly)
{
  widgets.reserve(5);

  /* So we want 3 vertical areas:
   * - the "central widget" (to be set later)
   * - an error area
   * - the button bar "edit" / "cancel"+"submit"
   */
  // The central widget
  centralWidget = new QWidget(this);

  // The errors area
  errorArea = new QWidget(this);

  // The button bar
  QWidget *buttonBar = new QWidget(this);
  editButton = new QPushButton(tr("edit"), buttonBar);
  QObject::connect(editButton, &QPushButton::clicked, this, &AtomicForm::wantEdit);
  cancelButton = new QPushButton(tr("cancel"), buttonBar);
  QObject::connect(cancelButton, &QPushButton::clicked, this, &AtomicForm::wantCancel);
  cancelButton->setEnabled(false);
  submitButton = new QPushButton(tr("submit"), buttonBar);
  QObject::connect(submitButton, &QPushButton::clicked, this, &AtomicForm::wantSubmit);
  submitButton->setEnabled(false);
  QHBoxLayout *buttonsLayout = new QHBoxLayout(buttonBar);
  buttonsLayout->addWidget(editButton);
  buttonsLayout->addWidget(cancelButton);
  buttonsLayout->addWidget(submitButton);

  groupLayout = new QVBoxLayout(this);
  groupLayout->addWidget(centralWidget);
  groupLayout->addWidget(errorArea);
  groupLayout->addWidget(buttonBar);

  /* Also prepare the confirmation dialog: */
  confirmationDialog = new QMessageBox();
  confirmationDialog->setText("Some values have been modified.");
  confirmationDialog->setInformativeText("Are you sure you want to cancel?");
  confirmationDialog->setStandardButtons(QMessageBox::Yes | QMessageBox::No);
  confirmationDialog->setDefaultButton(QMessageBox::No);
}

AtomicForm::~AtomicForm()
{
  // TODO: unlock whatever widget is locked
  delete centralWidget;
  delete errorArea;
  delete editButton;
  delete cancelButton;
  delete submitButton;
  delete groupLayout;
  delete confirmationDialog;
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
  KValue &kv = conf::kvs[aw->key];
  widgets.push_back(aw);
  QObject::connect(&kv, &KValue::valueLocked, this, &AtomicForm::lockValue);
  QObject::connect(&kv, &KValue::valueUnlocked, this, &AtomicForm::unlockValue);
}

void AtomicForm::lockAll()
{
  std::cerr << "lock all!" << std::endl;
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
    if (*aw->initValue != *v) {
      std::cerr << "value of " << aw->key << " has changed from "
                << aw->initValue << " to " << v << std::endl;
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
    if (*v != *aw->initValue) conf::askSet(aw->key, v);
    conf::askUnlock(aw->key);
  }
}

void AtomicForm::wantSubmit()
{
  if (someEdited()) {
    doSubmit();
  } else {
    std::cerr << "Cancelling rather, as no edition was done." << std::endl;
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
  std::cerr << "locked key " << k << " to user " << u.toStdString() << std::endl;
  if (u == conf::my_uid) {
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
