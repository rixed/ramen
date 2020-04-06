#include <cassert>
#include <QDebug>
#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include <QComboBox>
#include <QStackedLayout>
#include "AlertInfo.h"
#include "AlertInfoEditor.h"
#include "CloneDialog.h"
#include "CodeEdit.h"
#include "conf.h"
#include "SourceInfoViewer.h"
#include "KTextEdit.h"
#include "ProgramItem.h"

#include "CodeEditForm.h"

static bool const verbose(false);

CodeEditForm::CodeEditForm(QWidget *parent)
  : AtomicForm(true, parent)
{
  layout()->setContentsMargins(QMargins());

  QPushButton *cloneButton = new QPushButton("&Clone…");
  // Because that AtomicForm was created with buttons just above
  assert(buttonsLayout);
  buttonsLayout->insertWidget(0, cloneButton);

  codeEdit = new CodeEdit;
  codeEdit->setObjectName("codeEdit");
  // FIXME: codeEdit should inherit AtomicWidgetAlternative
  setCentralWidget(codeEdit);
  addWidget(codeEdit->alertEditor, true);
  addWidget(codeEdit->textEditor, true);
  addWidget(codeEdit->infoEditor, true);

  // Connect the clone button to the creation of a cloning dialog:
  connect(cloneButton, &QPushButton::clicked,
          this, &CodeEditForm::wantClone);
  // Disable language switcher in edit mode
  connect(this, &CodeEditForm::changeEnabled,
          codeEdit, &CodeEdit::disableLanguageSwitch);
}

void CodeEditForm::wantClone()
{
  if (verbose)
    qDebug() << "CodeEditForm::wantClone: keyPrefix="
             << QString::fromStdString(codeEdit->keyPrefix)
             << ", extension=" << codeEdit->extensionsCombo->currentData().toString();

  if (codeEdit->keyPrefix.empty()) return;

  /* We might want to have as many of those dialogs open as we want
   * to create clones (possibly of the same source); So just create
   * a new dialog each time the clone button is clicked.
   * Note that we clone only the selected extension. */
  std::string const orig(
    codeEdit->keyPrefix + "/" +
    codeEdit->extensionsCombo->currentData().toString().toStdString());

  CloneDialog *dialog(new CloneDialog(orig, this));
  dialog->show();
}
