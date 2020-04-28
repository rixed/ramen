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
  extensionsCombo->setObjectName("extensionsCombo");
  connect(extensionsCombo, QOverload<const QString &>::of(
                             &QComboBox::currentIndexChanged),
          this, &CodeEdit::inputChanged);

  stackedLayout = new QStackedLayout;
  stackedLayout->setObjectName("stackedLayout");

  alertEditor = new AlertInfoEditor;
  alertEditor->setObjectName("alertEditor");
  connect(alertEditor, &AlertInfoEditor::inputChanged,
          this, &CodeEdit::inputChanged);

  /* Beware: Same indices are used to access currentWidget, stackedLayout,
   * extensionsCombo: */
  alertEditorIndex = stackedLayout->addWidget(alertEditor);
  extensionsCombo->addItem(tr("Simple Alert"), "alert");

  textEditor = new KTextEdit;
  textEditor->setObjectName("textEditor");
  connect(textEditor, &KTextEdit::inputChanged,
          this, &CodeEdit::inputChanged);

  textEditorIndex = stackedLayout->addWidget(textEditor);
  extensionsCombo->addItem(tr("Ramen Language"), "ramen");

  infoEditor = new SourceInfoViewer;
  infoEditor->setObjectName("infoEditor");
  infoEditorIndex = stackedLayout->addWidget(infoEditor);
  extensionsCombo->addItem(tr("Informations"), "info");

  QFormLayout *switcherLayout = new QFormLayout;
  switcherLayout->setObjectName("switcherLayout");
  switcherLayout->addRow(
    tr("At which level do you want to edit this program?"),
    extensionsCombo);
  extensionSwitcher = new QWidget;
  extensionSwitcher->setObjectName("extensionSwitcher");
  extensionSwitcher->setLayout(switcherLayout);

  compilationError = new QLabel;
  compilationError->setObjectName("compilationError");
  compilationError->setWordWrap(true);
  compilationError->hide();

  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  layout->addWidget(extensionSwitcher);
  layout->addLayout(stackedLayout);
  layout->addWidget(compilationError);
  setLayout(layout);

  // Connect the error label to this hide/show slot
  connect(kvs, &KVStore::keyChanged,
          this, &CodeEdit::onChange);

  // Display the corresponding widget when the extension combobox changes:
  connect(extensionsCombo, QOverload<int>::of(&QComboBox::currentIndexChanged),
          this, &CodeEdit::setLanguage);
}

void CodeEdit::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        setError(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

AtomicWidget const *CodeEdit::currentWidget() const
{
  int const editorIndex(extensionsCombo->currentIndex());

  if (editorIndex == textEditorIndex) {
    return textEditor;
  } else if (editorIndex == alertEditorIndex) {
    return alertEditor;
  } else if (editorIndex == infoEditorIndex) {
    return infoEditor;
  }

  qFatal("CodeEdit: invalid editorIndex=%d", editorIndex);
}

std::shared_ptr<conf::Value const> CodeEdit::getValue() const
{
  return currentWidget()->getValue();
}

bool CodeEdit::hasValidInput() const
{
  return currentWidget()->hasValidInput();
}

void CodeEdit::enableLanguage(int index, bool enabled)
{
  QStandardItemModel *model(
    static_cast<QStandardItemModel *>(extensionsCombo->model()));

  if (enabled != model->item(index)->isEnabled()) {
    if (verbose)
      qDebug() << "CodeEdit: language" << index
               << (extensionsCombo->currentIndex() == index ? " (current)":"")
               << "is now" << enabled;

    model->item(index)->setEnabled(enabled);
  }

  /* As much as possible we want the current selected language to be enabled.
   * That's how the form switch to a valid language when selecting another
   * source. */
  if (! model->item(extensionsCombo->currentIndex())->isEnabled()) {
    for (int row = 0; row < model->rowCount(); row++) {
      if (model->item(row)->isEnabled()) {
        if (verbose)
          qDebug() << "CodeEdit: switching extension to" << row;
        extensionsCombo->setCurrentIndex(row);
        break;
      }
    }
  }
}

void CodeEdit::setLanguageKey(
  int index, AtomicWidget *editor, std::string const &key)
{
  if (verbose)
    qDebug() << "CodeEdit: set language key for index" << index << "to"
             << QString::fromStdString(key);

  enableLanguage(index, !key.empty());
  editor->setKey(key);
}

void CodeEdit::setLanguage(int index)
{
  if (verbose)
    qDebug() << "CodeEdit: Switching to language" << index;

  stackedLayout->setCurrentIndex(index);
  /* Useful when called by NewSourceDialog but not so much when signaling
   * currentIndexChanged: */
  extensionsCombo->setCurrentIndex(index);
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

  kvs->lock.lock_shared();
  KValue const *kv = nullptr;

  /* When the key prefix is set (ie. the view switch to this source) select
   * by default the language that has been edited most recently by a human: */
  double latest_mtime { 0 };
  int latest_index = textEditorIndex;  // default is ramen
  std::function<void(double mtime, QString const &uid, int index)> const
    compete_latest = [&latest_mtime, &latest_index]
      (double mtime, QString const &uid, int index) {
        if (uid.size() == 0 || uid.at(0) == '_') return;
        if (mtime >= latest_mtime) {
          latest_mtime = mtime;
          latest_index = index;
        }
  };

  // Look for the alert first:
  auto it = kvs->map.find(alertKey);
  if (it != kvs->map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found an alert";
    setLanguageKey(alertEditorIndex, alertEditor, alertKey);
    numSources ++;
    compete_latest(it->second.mtime, it->second.uid, alertEditorIndex);
  } else {
    setLanguageKey(alertEditorIndex, alertEditor, std::string());
  }

  // Then look for the ramen source that is the second best option:
  it = kvs->map.find(ramenKey);
  if (it != kvs->map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found ramen code";
    setLanguageKey(textEditorIndex, textEditor, ramenKey);
    numSources ++;
    compete_latest(it->second.mtime, it->second.uid, textEditorIndex);
  } else {
    setLanguageKey(textEditorIndex, textEditor, std::string());
  }

  // When all else fails, display the (non-editable) info:
  it = kvs->map.find(infoKey);
  if (it != kvs->map.end() &&
      /* Skip Null values that are created as placeholder during compilation: */
      !it->second.val->isNull()) {
    if (verbose)
      qDebug() << "CodeEdit::setKeyPrefix: found info";
    setLanguageKey(infoEditorIndex, infoEditor, infoKey);
    numSources ++;
    // To show error messages prominently regardless of the current editor:
    kv = &it->second;
    /* Info will be the latest edit but it's not a source language:
     * compete_latest(it->second.mtime, infoEditorIndex); */
  } else {
    // Disable that language
    setLanguageKey(infoEditorIndex, infoEditor, std::string());
  }

  extensionSwitcher->setVisible(numSources > 1);
  extensionsCombo->setCurrentIndex(latest_index);

  resetError(kv);
  kvs->lock.unlock_shared();
}

void CodeEdit::disableLanguageSwitch(bool disabled)
{
  extensionsCombo->setEnabled(!disabled);
}
