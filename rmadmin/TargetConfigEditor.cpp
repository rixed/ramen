#include <iostream>
#include <cassert>
#include "confRCEntry.h"
#include "RCEntryEditor.h"
#include "TargetConfigEditor.h"

TargetConfigEditor::TargetConfigEditor(conf::Key const &key, QWidget *parent) :
  AtomicWidget(key, parent)
{
  toolBox = new QToolBox;
  setCentralWidget(toolBox);

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &TargetConfigEditor::setValue);
  connect(&kv, &KValue::valueChanged, this, &TargetConfigEditor::setValue);
  connect(&kv, &KValue::valueLocked, this, &TargetConfigEditor::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &TargetConfigEditor::unlockValue);
}

std::shared_ptr<conf::Value const> TargetConfigEditor::getValue() const
{
  std::shared_ptr<conf::TargetConfig> rc(new conf::TargetConfig());

  // Rebuilt the whole RC from the form:
  for (int i = 0; i < toolBox->count(); i++) {
    RCEntryEditor const *entry =
      dynamic_cast<RCEntryEditor const *>(toolBox->widget(i));
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
  for (int i = 0; i < toolBox->count(); i++) {
    RCEntryEditor *entry = dynamic_cast<RCEntryEditor *>(toolBox->widget(i));
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
  while (toolBox->count() > 0) {
    QWidget *w = toolBox->widget(0);
    toolBox->removeItem(0);
    w->deleteLater();
  }

  for (auto const &it : rc->entries) {
    conf::RCEntry const *entry = it.second;
    RCEntryEditor *entryEditor = new RCEntryEditor(entry);
    toolBox->addItem(entryEditor, QString::fromStdString(it.first));
  }

  emit valueChanged(k, v);

  return true;
}
