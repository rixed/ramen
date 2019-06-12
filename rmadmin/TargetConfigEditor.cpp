#include "confRCEntry.h"
#include "RCEntryEditor.h"
#include "TargetConfigEditor.h"

TargetConfigEditor::TargetConfigEditor(std::string const &key, QWidget *parent) :
  QToolBox(parent),
  AtomicWidget(key)
{
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();
  connect(&kv, &KValue::valueCreated, this, &TargetConfigEditor::setValue);
  connect(&kv, &KValue::valueChanged, this, &TargetConfigEditor::setValue);
  connect(&kv, &KValue::valueLocked, this, &TargetConfigEditor::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &TargetConfigEditor::unlockValue);
  if (kv.isSet()) setValue(key, kv.value());
}

std::shared_ptr<conf::Value const> TargetConfigEditor::getValue() const
{
  return nullptr;  // TODO
}

void TargetConfigEditor::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  // TODO
}

void TargetConfigEditor::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::TargetConfig const> rc =
    std::dynamic_pointer_cast<conf::TargetConfig const>(v);
  if (! rc) {
    std::cout << "Target config not of TargetConfig type!?" << std::endl;
    return;
  }

  /* Since we have a single value and it is locked whenever we want to edit
   * it, the current form cannot have any modification when a new value is
   * received. Therefore there is no use for preserving current values: */
  while (count() > 0) {
    QWidget *w = widget(0);
    removeItem(0);
    w->deleteLater();
  }

  for (auto const &it : rc->entries) {
    conf::RCEntry const *entry = it.second;
    RCEntryEditor *entryEditor = new RCEntryEditor(entry);
    addItem(entryEditor, QString::fromStdString(it.first));
  }
}
