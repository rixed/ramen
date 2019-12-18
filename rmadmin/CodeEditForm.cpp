#include <QDebug>
#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include <QComboBox>
#include <QStackedLayout>
#include "AtomicWidgetAlternative.h"
#include "AtomicForm.h"
#include "ProgramItem.h"
#include "AlertInfo.h"
#include "conf.h"
#include "CloneDialog.h"
#include "CodeEdit.h"
#include "CodeEditForm.h"

static bool const verbose = false;

CodeEditForm::CodeEditForm(QWidget *parent) :
  QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  editorForm = new AtomicForm(this);
  layout->addWidget(editorForm);

  QPushButton *cloneButton = new QPushButton("&Clone…");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  codeEdit = new CodeEdit;
  // FIXME: codeEdit should inherit AtomicWidgetAlternative
  editorForm->setCentralWidget(codeEdit);
  editorForm->addWidget(codeEdit->editor, true);

  // Connect the clone button to the creation of a cloning dialog:
  connect(cloneButton, &QPushButton::clicked,
          this, &CodeEditForm::wantClone);
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
  std::string orig =
    codeEdit->keyPrefix +"/" + codeEdit->extensionsCombo->currentData().toString().toStdString();
  CloneDialog *dialog = new CloneDialog(orig, this);
  dialog->show();
}
