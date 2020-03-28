#include <QtGlobal>
#include <QDebug>
#include <QFormLayout>
#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include <QComboBox>
#include <QStackedLayout>
#include "KTextEdit.h"
#include "AtomicWidgetAlternative.h"
#include "ProgramItem.h"
#include "AlertInfoEditor.h"
#include "conf.h"
#include "CodeEdit.h"

static bool const verbose(false);

CodeEdit::CodeEdit(QWidget *parent) :
  AtomicWidgetAlternative(parent)
{
  extensionsCombo = new QComboBox;
  stackedLayout = new QStackedLayout;

  textEditor = new KTextEdit;
  addWidget(textEditor);
  /* Beware: Same indices are used to access currentWidget, stackedLayout,
   * extensionsCombo: */
  textEditorIndex = stackedLayout->addWidget(textEditor);
  extensionsCombo->addItem(tr("Ramen Language"), "ramen");

  alertEditor = new AlertInfoEditor;
  addWidget(alertEditor);
  alertEditorIndex = stackedLayout->addWidget(alertEditor);
  extensionsCombo->addItem(tr("Simple Alert"), "alert");

  QFormLayout *switcherLayout = new QFormLayout;
  switcherLayout->addRow(
    tr("At which level do you want to edit this program?"),
    extensionsCombo);
  extensionSwitcher = new QWidget;
  extensionSwitcher->setLayout(switcherLayout);

  compilationError = new QLabel;
  compilationError->setWordWrap(true);
  compilationError->hide();

  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  layout->addWidget(extensionSwitcher);
  layout->addLayout(stackedLayout);
  layout->addWidget(compilationError);
  QWidget *widget = new QWidget(this);
  widget->setLayout(layout);
  relayoutWidget(widget);

  // Connect the error label to this hide/show slot
  connect(&kvs, &KVStore::valueCreated, this, &CodeEdit::setError);
  connect(&kvs, &KVStore::valueChanged, this, &CodeEdit::setError);

  // Display the corresponding widget when the extension combobox changes:
  connect(extensionsCombo, QOverload<int>::of(&QComboBox::currentIndexChanged),
          this, &CodeEdit::setLanguage);
}

void CodeEdit::setLanguage(int index)
{
  if (verbose)
    qDebug() << "CodeEdit: Switching to language" << index;

  setCurrentWidget(index);
  stackedLayout->setCurrentIndex(index);
}

void CodeEdit::setError(std::string const &key, KValue const &kv)
{
  if (key != keyPrefix + "/info") return;
  doResetError(kv);
}

void CodeEdit::doResetError(KValue const &kv)
{
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.val);
  if (info) {
    compilationError->setText(stringOfDate(kv.mtime) + ": " + info->errMsg);
    compilationError->setVisible(! info->errMsg.isEmpty());
  } else {
    qCritical() << QString::fromStdString(keyPrefix)
                << "/info is not a SourceInfo?!";
  }
}

void CodeEdit::resetError(KValue const *kv)
{
  if (kv) {
    doResetError(*kv);
  } else {
    compilationError->setText(tr("Not compiled yet"));
    compilationError->setVisible(true);
  }
}

void CodeEdit::setKeyPrefix(std::string const &prefix)
{
  if (verbose)
    qDebug() << "CodeEdit::setKeyPrefix:" << QString::fromStdString(prefix)
             << "replacing " << QString::fromStdString(keyPrefix);

  if (keyPrefix == prefix) return;
  keyPrefix = prefix;

  std::string const alertKey = prefix + "/alert";
  std::string const ramenKey = prefix + "/ramen";
  std::string const infoKey = prefix + "/info";

  unsigned numSources = 0;

  kvs.lock.lock_shared();
  KValue const *kv = nullptr;
  auto it = kvs.map.find(infoKey);
  if (it != kvs.map.end()) kv = &it->second;
  // Look for the ramen source:
  it = kvs.map.find(ramenKey);
  if (it != kvs.map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      ! it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found ramen code";
    setCurrentWidget(textEditorIndex);
    setKey(ramenKey);
    stackedLayout->setCurrentIndex(textEditorIndex);
    extensionsCombo->setCurrentIndex(textEditorIndex);
    numSources ++;
  }
  // Takes precedence over the ramen source:
  it = kvs.map.find(alertKey);
  if (it != kvs.map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      ! it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found an alert";
    setCurrentWidget(alertEditorIndex);
    setKey(alertKey);
    stackedLayout->setCurrentIndex(alertEditorIndex);
    extensionsCombo->setCurrentIndex(alertEditorIndex);
    numSources ++;
  }
  extensionSwitcher->setVisible(numSources > 1);
  resetError(kv);
  kvs.lock.unlock_shared();
}

void CodeEdit::setEnabled(bool enabled)
{
  // Prevent the extension to switch during edition:
  extensionsCombo->setEnabled(!enabled);
  AtomicWidgetAlternative::setEnabled(enabled);
}
