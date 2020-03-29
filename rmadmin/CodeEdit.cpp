#include <QDebug>
#include <QComboBox>
#include <QFormLayout>
#include <QLabel>
#include <QPushButton>
#include <QStackedLayout>
#include <QStandardItemModel>
#include <QVBoxLayout>
#include <QtGlobal>
#include "KTextEdit.h"
#include "ProgramItem.h"
#include "AlertInfoEditor.h"
#include "conf.h"
#include "SourceInfoViewer.h"

#include "CodeEdit.h"

static bool const verbose(false);

CodeEdit::CodeEdit(QWidget *parent) :
  QWidget(parent)
{
  extensionsCombo = new QComboBox;
  stackedLayout = new QStackedLayout;

  alertEditor = new AlertInfoEditor;
  /* Beware: Same indices are used to access currentWidget, stackedLayout,
   * extensionsCombo: */
  alertEditorIndex = stackedLayout->addWidget(alertEditor);
  extensionsCombo->addItem(tr("Simple Alert"), "alert");

  textEditor = new KTextEdit;
  textEditorIndex = stackedLayout->addWidget(textEditor);
  extensionsCombo->addItem(tr("Ramen Language"), "ramen");

  infoEditor = new SourceInfoViewer;
  infoEditorIndex = stackedLayout->addWidget(infoEditor);
  extensionsCombo->addItem(tr("Informations"), "info");

  // Default should be ramen:
  setLanguage(textEditorIndex);

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
  setLayout(layout);

  // Connect the error label to this hide/show slot
  connect(&kvs, &KVStore::valueCreated, this, &CodeEdit::setError);
  connect(&kvs, &KVStore::valueChanged, this, &CodeEdit::setError);

  // Display the corresponding widget when the extension combobox changes:
  connect(extensionsCombo, QOverload<int>::of(&QComboBox::currentIndexChanged),
          this, &CodeEdit::setLanguage);
}

std::shared_ptr<conf::Value const> CodeEdit::getValue() const
{
  int const editorIndex(extensionsCombo->currentIndex());

  if (editorIndex == textEditorIndex) {
    return textEditor->getValue();
  } else if (editorIndex == alertEditorIndex) {
    return alertEditor->getValue();
  } else if (editorIndex == infoEditorIndex) {
    return infoEditor->getValue();
  }

  qFatal("CodeEdit::getValue while editorIndex=%d", editorIndex);
}

void CodeEdit::enableLanguage(int index, bool enabled)
{
  QStandardItemModel *model(
    static_cast<QStandardItemModel *>(extensionsCombo->model()));
  model->item(index)->setEnabled(enabled);
}

void CodeEdit::setLanguage(int index)
{
  if (verbose)
    qDebug() << "CodeEdit: Switching to language" << index;

  extensionsCombo->setCurrentIndex(index);
  stackedLayout->setCurrentIndex(index);
  enableLanguage(index, true);
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

  enableLanguage(infoEditorIndex, false);
  enableLanguage(textEditorIndex, false);
  enableLanguage(alertEditorIndex, false);

  kvs.lock.lock_shared();
  KValue const *kv = nullptr;
  auto it = kvs.map.find(infoKey);
  if (it != kvs.map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found info";
    setLanguage(infoEditorIndex);
    infoEditor->setKey(infoKey);
    numSources ++;
    // To show error messages prominently regardless of the current editor:
    kv = &it->second;
  }

  // Look for the ramen source that would take precedence over the info widget:
  it = kvs.map.find(ramenKey);
  if (it != kvs.map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found ramen code";
    setLanguage(textEditorIndex);
    textEditor->setKey(ramenKey);
    numSources ++;
  }

  // Look for the alert that would take precedence over the ramen source:
  it = kvs.map.find(alertKey);
  if (it != kvs.map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found an alert";
    setLanguage(alertEditorIndex);
    alertEditor->setKey(alertKey);
    numSources ++;
  }

  extensionSwitcher->setVisible(numSources > 1);
  resetError(kv);
  kvs.lock.unlock_shared();
}
