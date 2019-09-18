#include <iostream>
#include <cassert>
#include <QTabWidget>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QComboBox>
#include "confRCEntry.h"
#include "RCEntryEditor.h"
#include "confValue.h"
#include "TargetConfigEditor.h"

static bool const verbose = true;

TargetConfigEditor::TargetConfigEditor(QWidget *parent) :
  AtomicWidget(parent)
{
  rcEntries = new QTabWidget;
  relayoutWidget(rcEntries);
}

std::shared_ptr<conf::Value const> TargetConfigEditor::getValue() const
{
  std::shared_ptr<conf::TargetConfig> rc(new conf::TargetConfig());

  // Rebuilt the whole RC from the form:
  for (int i = 0; i < rcEntries->count(); i++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(i));
    if (! entry) {
      std::cerr << "TargetConfigEditor entry " << i << " not a RCEntryEditor?!" << std::endl;
      continue;
    }
    rc->addEntry(entry->getValue());
  }
  return rc;
}

void TargetConfigEditor::setEnabled(bool enabled)
{
  if (verbose)
    std::cout << "TargetConfigEditor::setEnabled(" << enabled << ")" << std::endl;

  if (verbose)
    std::cout << "TargetConfigEditor::setEnabled: ... propagating to "
              << rcEntries->count() << " rc-entries" << std::endl;

  for (int i = 0; i < rcEntries->count(); i++) {
    RCEntryEditor *entry = dynamic_cast<RCEntryEditor *>(rcEntries->widget(i));
    if (! entry) {
      std::cerr << "TargetConfigEditor: widget " << i << " not an RCEntryEditor?!"
                << std::endl;
      continue;
    }
    entry->setEnabled(enabled);
  }
}

bool TargetConfigEditor::setValue(std::string const &k, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::TargetConfig const> rc =
    std::dynamic_pointer_cast<conf::TargetConfig const>(v);
  if (! rc) {
    std::cerr << "Target config not of TargetConfig type!?" << std::endl;
    return false;
  }

  /* Since we have a single value and it is locked whenever we want to edit
   * it, the current form cannot have any modification when a new value is
   * received. Therefore there is no use for preserving current values: */
  while (rcEntries->count() > 0) {
    QWidget *w = rcEntries->widget(0);
    rcEntries->removeTab(0);
    w->deleteLater();
  }

  for (auto const &it : rc->entries) {
    conf::RCEntry const *entry = it.second;
    RCEntryEditor *entryEditor = new RCEntryEditor(true);
    entryEditor->setProgramName(it.first);
    entryEditor->setValue(entry);
    rcEntries->addTab(entryEditor, QString::fromStdString(it.first));

    connect(entryEditor, &RCEntryEditor::inputChanged,
            this, &TargetConfigEditor::inputChanged);
  }

  emit valueChanged(k, v);

  return true;
}

RCEntryEditor const *TargetConfigEditor::currentEntry() const
{
  RCEntryEditor const *entry =
    dynamic_cast<RCEntryEditor const *>(rcEntries->currentWidget());
  if (! entry) {
    std::cerr << "TargetConfigEditor entry that's not a RCEntryEditor?!"
              << std::endl;
  }
  return entry;
}

void TargetConfigEditor::removeEntry(RCEntryEditor const *toRemove)
{
  for (int c = 0; c < rcEntries->count(); c ++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(c));
    if (! entry) continue;
    if (entry == toRemove) {
      rcEntries->removeTab(c);
      return;
    }
  }

  std::cerr << "Asked to remove entry @" << toRemove << " but coundn't find it" << std::endl;
}

void TargetConfigEditor::preselect(QString const &programName)
{
  std::string const pName = programName.toStdString();
  QString const srcPath =
    QString::fromStdString(srcPathFromProgramName(pName));
  QString const programSuffix =
    QString::fromStdString(suffixFromProgramName(pName));

  for (int c = 0; c < rcEntries->count(); c ++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(c));
    if (! entry) continue;
    if (entry->suffixEdit->text() == programSuffix &&
        entry->sourceBox->currentText() == srcPath
    ) {
      rcEntries->setCurrentIndex(c);
      return;
    }
  }

  std::cerr << "Could not preselect program " << programName.toStdString() << std::endl;
}
