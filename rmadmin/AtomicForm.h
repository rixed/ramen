#ifndef ATOMICFORM_H_190504
#define ATOMICFORM_H_190504
#include <vector>
#include <set>
#include <QWidget>
#include <QString>
#include <QVBoxLayout>
#include <QPushButton>
#include <QGroupBox>
#include <QMessageBox>
#include "confValue.h"
#include "confKey.h"
#include "AtomicWidget.h"

/* We want to be able to edit a group of values atomically.
 * For this, we need this group of widget to be associated with 3 buttons:
 * "edit", "cancel" and "commit".
 *
 * We are passed a list of KWidgets (Keyed-Widget), which must have a
 * setEnabled(bool) slot (all QWidgets have this one),
 * and they must have a method to get the confValue out of it, that we call
 * when we commit the form and when we click "edit" to save the initial
 * values.
 *
 * Then this class will lock all the keys (then wait for them to be actually
 * locked before making the widget editable (thanks to the notifications)
 * and the cancel/submit buttons clickable), display errors, etc.
 *
 * Regarding the layout, this class does not mess with the layout of the
 * passed widgets (assuming they are laid out already).
 * The simpler would be to build it like the MainApplication window is build:
 * ie we have an empty widget where the user add one by one the widgets that
 * he want, some of them being atomic widgets and some of them being normal
 * widgets. And then atomic form adds ots buttons below.
 * We therefore just offer an "add" method, and use introspection to know
 * it the user adds an atomic widget to register it in the atomic group.
 */

class AtomicForm : public QGroupBox
{
  Q_OBJECT

  std::vector<AtomicWidget *> widgets;
  std::set<AtomicWidget *> deletables;

  QVBoxLayout *groupLayout;
  QWidget *errorArea;
  QPushButton *editButton, *cancelButton, *deleteButton, *submitButton;
  QMessageBox *confirmCancelDialog, *confirmDeleteDialog;

  // The set of all keys currently locked by this user:
  std::set<conf::Key> locked;

  void wantEdit();
  void wantCancel();
  void wantDelete();
  void wantSubmit();
  void doCancel();
  void doSubmit();
  bool someEdited();

public:
  QWidget *centralWidget;

  AtomicForm(QWidget *parent = nullptr);

  AtomicForm(QString const &title, QWidget *parent = nullptr) :
    AtomicForm(parent)
  {
    setTitle(title);
  }

  ~AtomicForm();

  void setCentralWidget(QWidget *);
  // and take ownership of those QWidgets:
  void addWidget(AtomicWidget *, bool deletable = false);

  // In case one want to add buttons in there:
  QHBoxLayout *buttonsLayout;

public slots:
  void setEnabled(bool);
  void setValue(conf::Key const &, conf::Value const &) {}
  void lockValue(conf::Key const &, QString const &);
  void unlockValue(conf::Key const &);
  void changeKey(conf::Key const &oldKey, conf::Key const &newKey);

signals:
  void changeEnabled(bool);
};

#endif
