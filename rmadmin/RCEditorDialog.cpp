#include <QPushButton>
#include <QHBoxLayout>
#include <QMessageBox>
#include "TargetConfigEditor.h"
#include "AtomicForm.h"
#include "RCEntryEditor.h"
#include "RCEditorDialog.h"

RCEditorDialog::RCEditorDialog(QWidget *parent) :
  QMainWindow(parent)
{
  setUnifiedTitleAndToolBarOnMac(true);

  AtomicForm *form = new AtomicForm(this);

  /* Prepare to add a delete button to the form.
   * Notice that RC entries being just elements of the TargetConfig
   * they have no independent keys that could be deleted independently.
   * Instead, we need an ad-hoc delete function. */
  QPushButton *deleteButton = new QPushButton(tr("Delete this entry"));
  form->buttonsLayout->insertWidget(2, deleteButton);
  connect(form, &AtomicForm::changeEnabled,
          deleteButton, &QPushButton::setEnabled);
  /* TODO: connect the delete button into a deleteEntry slot that
   * will ask for confirmation, and then edit not the TargetConfig value
   * but the TargetConfig form, so that once we submit the new value does
   * not include the deleted entries. */
  connect(deleteButton, &QPushButton::clicked,
          this, &RCEditorDialog::wantDeleteEntry);

  targetConfigEditor = new TargetConfigEditor;
  targetConfigEditor->setKey(conf::Key("target_config"));
  form->setCentralWidget(targetConfigEditor);
  form->addWidget(targetConfigEditor);

  setCentralWidget(form);
  setWindowTitle(tr("Running Configuration"));

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
    info.append("\n\nAlternatively, it could be just temporarily disabled.");
  confirmDeleteDialog->setInformativeText(info);

  if (QMessageBox::Yes == confirmDeleteDialog->exec()) {
    targetConfigEditor->removeEntry(currentEntry);
  }
}

void RCEditorDialog::preselect(QString const &programName)
{
  targetConfigEditor->preselect(programName);
}