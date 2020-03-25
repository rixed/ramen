#include <cassert>
#include <QtGlobal>
#include <QComboBox>
#include <QDebug>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QComboBox>
#include <QLabel>
#include <QStackedLayout>
#include "confRCEntry.h"
#include "RCEntryEditor.h"
#include "confValue.h"
#include "TargetConfigEditor.h"

static bool const verbose(false);

TargetConfigEditor::TargetConfigEditor(QWidget *parent) :
  AtomicWidget(parent),
  currentIndex(-1)
{
  rcEntries.reserve(10);

  entrySelector = new QComboBox;
  entryEditor = new RCEntryEditor(true);
  noSelectionText = new QLabel(tr("No programs are running.\n"
                                  "Press ⌘ R to run a program."));
  stackedLayout = new QStackedLayout;
  entryEditorIdx = stackedLayout->addWidget(entryEditor);
  noSelectionIdx = stackedLayout->addWidget(noSelectionText);
  stackedLayout->setCurrentIndex(noSelectionIdx);

  QWidget *w = new QWidget;
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(entrySelector);
  layout->addLayout(stackedLayout);
  w->setLayout(layout);
  relayoutWidget(w);

  connect(entryEditor, &RCEntryEditor::inputChanged,
          this, &TargetConfigEditor::inputChanged);

  connect(entrySelector, QOverload<int>::of(&QComboBox::currentIndexChanged),
          this, &TargetConfigEditor::changeEntry);
}

std::shared_ptr<conf::Value const> TargetConfigEditor::getValue() const
{
  std::shared_ptr<conf::TargetConfig> rc(new conf::TargetConfig());

  for (int i = 0; i < (int)rcEntries.size(); i ++) {
    rc->addEntry(
      i == currentIndex ?
        std::shared_ptr<conf::RCEntry>(entryEditor->getValue()) :
        rcEntries[i]);
  }

  return rc;
}

/*
  // Rebuilt the whole RC from the form:
  for (int i = 0; i < rcEntries->rowCount(); i++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(i));
    if (! entry) {
      qCritical() << "TargetConfigEditor entry" << i << "not a RCEntryEditor?!";
      continue;
    }
    rc->addEntry(entry->getValue());
  }
*/

void TargetConfigEditor::setEnabled(bool enabled)
{
  if (verbose)
    qDebug() << "TargetConfigEditor::setEnabled(" << enabled << ")";

  entryEditor->setEnabled(enabled);
}

/*
  for (int i = 0; i < rcEntries->count(); i++) {
    RCEntryEditor *entry = dynamic_cast<RCEntryEditor *>(rcEntries->widget(i));
    if (! entry) {
      qCritical() << "TargetConfigEditor: widget" << i << "not an RCEntryEditor?!";
      continue;
    }
    entry->setEnabled(enabled);
  }
*/

bool TargetConfigEditor::setValue(std::string const &k, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::TargetConfig const> rc =
    std::dynamic_pointer_cast<conf::TargetConfig const>(v);
  if (! rc) {
    qCritical() << "Target config not of TargetConfig type!?";
    return false;
  }

  /* Since we have a single value and it is locked whenever we want to edit
   * it, the current widget cannot have any modification when a new value is
   * received. Therefore there is no use for preserving current values: */
  /* Note that, due to the currentIndexChanged signal, we must unselect first
   * and then empty: */
  entrySelector->setCurrentIndex(-1);
  rcEntries.clear();
  while (entrySelector->count() > 0) entrySelector->removeItem(0);

  for (auto const &it : rc->entries) {
    /* The entry must be present in rcEntries array before urrentIndexChanged
     * is signalled! */
    rcEntries.push_back(it.second);
    entrySelector->addItem(QString::fromStdString(it.first));
  }

  assert(entrySelector->count() == (int)rcEntries.size());

  if (entrySelector->count() > 0) {
    stackedLayout->setCurrentIndex(entryEditorIdx);
    currentIndex = 0;
    entryEditor->setValue(*rcEntries[currentIndex]);
    entrySelector->setCurrentIndex(currentIndex);
  } else {
    stackedLayout->setCurrentIndex(noSelectionIdx);
    currentIndex = -1;
  }

/*
    RCEntryEditor *entryEditor = new RCEntryEditor(true);
    entryEditor->setProgramName(it.first);
    entryEditor->setValue(entry);
    rcEntries->addTab(entryEditor, QString::fromStdString(it.first));

    connect(entryEditor, &RCEntryEditor::inputChanged,
            this, &TargetConfigEditor::inputChanged);
*/

  emit valueChanged(k, v);

  return true;
}

/*
RCEntryEditor const *TargetConfigEditor::currentEntry() const
{

  RCEntryEditor const *entry =
    dynamic_cast<RCEntryEditor const *>(rcEntries->currentWidget());
  if (! entry) {
    qCritical() << "TargetConfigEditor entry that's not a RCEntryEditor?!";
  }
  return entry;
}
*/

void TargetConfigEditor::removeCurrentEntry()
{
  int const idx = entrySelector->currentIndex();
  assert(idx == currentIndex);

  if (stackedLayout->currentIndex() != entryEditorIdx ||
      idx < 0 || idx >= (int)rcEntries.size()) return;

  rcEntries.erase(rcEntries.begin()+idx);
  entrySelector->removeItem(idx);
  currentIndex = entrySelector->currentIndex();

/*
  for (int c = 0; c < rcEntries->count(); c ++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(c));
    if (! entry) continue;
    if (entry == toRemove) {
      rcEntries->removeTab(c);
      return;
    }
  }

  qCritical() << "Asked to remove entry @" << toRemove << "but coundn't find it";
*/
}

void TargetConfigEditor::preselect(QString const &programName)
{
  int const idx = entrySelector->findText(programName);
  if (idx < 0) {
    qCritical() << "Could not preselect program" << programName;
    return;
  }

  if (idx != entrySelector->currentIndex()) {
    entrySelector->setCurrentIndex(idx);
  }

/*
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

  qCritical() << "Could not preselect program" << programName;
  */
}

void TargetConfigEditor::changeEntry(int idx)
{
  if (currentIndex >= 0) {
    /* Save the value from the editor: */
    if (currentIndex < (int)rcEntries.size()) {
      rcEntries[currentIndex] =
        std::shared_ptr<conf::RCEntry>(entryEditor->getValue());
    } else {
      /* Can happen that currentIndex is right past the end if we deleted
       * the last entry: */
      assert(currentIndex == (int)rcEntries.size());
    }
  }

  currentIndex = idx;

  if (idx >= 0) {
    entryEditor->setValue(*rcEntries[idx]);
  }
}
