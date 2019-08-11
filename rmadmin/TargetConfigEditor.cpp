#include <iostream>
#include <cassert>
#include <QTabWidget>
#include <QVBoxLayout>
#include <QLineEdit>
#include "once.h"
#include "confRCEntry.h"
#include "RCEntryEditor.h"
#include "TargetConfigEditor.h"

TargetConfigEditor::TargetConfigEditor(QWidget *parent) :
  AtomicWidget(parent)
{
  rcEntries = new QTabWidget;
  relayoutWidget(rcEntries);
}

void TargetConfigEditor::extraConnections(KValue *kv)
{
  Once::connect(kv, &KValue::valueCreated, this, &TargetConfigEditor::setValue);
  connect(kv, &KValue::valueChanged, this, &TargetConfigEditor::setValue);
  connect(kv, &KValue::valueLocked, this, &TargetConfigEditor::lockValue);
  connect(kv, &KValue::valueUnlocked, this, &TargetConfigEditor::unlockValue);
}

std::shared_ptr<conf::Value const> TargetConfigEditor::getValue() const
{
  std::shared_ptr<conf::TargetConfig> rc(new conf::TargetConfig());

  // Rebuilt the whole RC from the form:
  for (int i = 0; i < rcEntries->count(); i++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(i));
    if (! entry) {
      std::cout << "TargetConfigEditor entry " << i << " not a RCEntryEditor?!" << std::endl;
      continue;
    }
    rc->addEntry(entry->getValue());
  }
  return rc;
}

void TargetConfigEditor::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  for (int i = 0; i < rcEntries->count(); i++) {
    RCEntryEditor *entry = dynamic_cast<RCEntryEditor *>(rcEntries->widget(i));
    if (! entry) continue;
    entry->setEnabled(enabled);
  }
}

bool TargetConfigEditor::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::TargetConfig const> rc =
    std::dynamic_pointer_cast<conf::TargetConfig const>(v);
  if (! rc) {
    std::cout << "Target config not of TargetConfig type!?" << std::endl;
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
    entryEditor->setSourceName(QString::fromStdString(entry->source));
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
    std::cout << "TargetConfigEditor entry that's not a RCEntryEditor?!" << std::endl;
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
  for (int c = 0; c < rcEntries->count(); c ++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(rcEntries->widget(c));
    if (! entry) continue;
    if (entry->nameEdit->text() == programName) {
      rcEntries->setCurrentIndex(c);
      return;
    }
  }

  std::cerr << "Could not preselect program " << programName.toStdString() << std::endl;
}
