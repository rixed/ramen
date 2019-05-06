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
  // TODO: set action!
  cancelButton = new QPushButton(tr("cancel"), buttonBar);
  // TODO: set action!
  cancelButton->setEnabled(false);
  submitButton = new QPushButton(tr("submit"), buttonBar);
  // TODO: set action!
  submitButton->setEnabled(false);
  QHBoxLayout *buttonsLayout = new QHBoxLayout(buttonBar);
  buttonsLayout->addWidget(editButton);
  buttonsLayout->addWidget(cancelButton);
  buttonsLayout->addWidget(submitButton);

  groupLayout = new QVBoxLayout(this);
  groupLayout->addWidget(centralWidget);
  groupLayout->addWidget(errorArea);
  groupLayout->addWidget(buttonBar);
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
}

void AtomicForm::setCentralWidget(QWidget *w)
{
  QLayoutItem *previous =
    groupLayout->replaceWidget(centralWidget, w, Qt::FindDirectChildrenOnly);
  assert(previous);
  delete previous;
}

void AtomicForm::addWidget(conf::Key const &key, QWidget *w)
{
  widgets.push_back(std::pair<conf::Key, QWidget *>(key, w));
  w->setEnabled(false);
  // TODO: also subscribe to those keys so that we can update our state and
  //       enable/disable the buttons
  KValue &kv = conf::kvs[key];
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
  for (std::pair<conf::Key, QWidget *> const w : widgets) {
    if (locked.find(w.first) == locked.end()) {
      conf::askLock(w.first);
    }
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
