#include <QPushButton>
#include <QHBoxLayout>
#include <QMessageBox>
#include "TargetConfigEditor.h"
#include "AtomicForm.h"
#include "RCEntryEditor.h"
#include "RCEditorDialog.h"

RCEditorDialog::RCEditorDialog(QWidget *parent) :
  SavedWindow("RCWindow", tr("Running Configuration"), parent)
{
  AtomicForm *form = new AtomicForm(this);

  /* Prepare to add a delete button to the form.
   * Notice that RC entries being just elements of the TargetConfig
   * they have no independent keys that could be deleted independently.
   * Instead, we need an ad-hoc delete function. */
  QPushButton *deleteButton = new QPushButton(tr("Delete this entry"));
  form->buttonsLayout->insertWidget(2, deleteButton);
  connect(form, &AtomicForm::changeEnabled,
          deleteButton, &QPushButton::setEnabled);
  // Every form start disabled (thus won't signal changeEnabled at init):
  deleteButton->setEnabled(form->isEnabled());

  connect(deleteButton, &QPushButton::clicked,
          this, &RCEditorDialog::wantDeleteEntry);

  targetConfigEditor = new TargetConfigEditor;
  targetConfigEditor->setKey(std::string("target_config"));
  form->setCentralWidget(targetConfigEditor);
  form->addWidget(targetConfigEditor);

  setCentralWidget(form);

  // Prepare the confirmatino dialog for deletion:
  confirmDeleteDialog = new QMessageBox(this);
  confirmDeleteDialog->setText("Are you sure you want to delete this entry?");
  confirmDeleteDialog->setStandardButtons(QMessageBox::Yes | QMessageBox::Cancel);
  confirmDeleteDialog->setDefaultButton(QMessageBox::Cancel);
  confirmDeleteDialog->setIcon(QMessageBox::Warning);
}

void RCEditorDialog::wantDeleteEntry()
{
  // Retrieve the entry.
  RCEntryEditor const *currentEntry = targetConfigEditor->currentEntry();
  if (! currentEntry) return;

  QString info(tr("This program will no longer be running."));
  if (currentEntry->programIsEnabled())
    info.append("\n\nAlternatively, this program could be "
                "temporarily disabled.");
  confirmDeleteDialog->setInformativeText(info);

  if (QMessageBox::Yes == confirmDeleteDialog->exec()) {
    targetConfigEditor->removeEntry(currentEntry);
  }
}

void RCEditorDialog::preselect(QString const &programName)
{
  targetConfigEditor->preselect(programName);
}
