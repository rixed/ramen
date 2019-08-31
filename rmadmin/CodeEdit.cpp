#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include <QComboBox>
#include <QStackedLayout>
#include "KTextEdit.h"
#include "AtomicWidgetAlternative.h"
#include "AtomicForm.h"
#include "ProgramItem.h"
#include "AlertInfo.h"
#include "conf.h"
#include "CodeEdit.h"

static bool const verbose = true;

CodeEdit::CodeEdit(QWidget *parent) :
  QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  editorForm = new AtomicForm(this);
  layout->addWidget(editorForm);

  QPushButton *cloneButton = new QPushButton("&Clone");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  textEditor = new KTextEdit;
  alertEditor = new AlertInfoEditor;
  editor = new AtomicWidgetAlternative;
  editor->addWidget(textEditor);
  editor->addWidget(alertEditor);
  // TODO: set the actually available options in setKeyPrefix:
  extensionsCombo = new QComboBox;
  extensionsCombo->addItem("alert");
  extensionsCombo->addItem("ramen");
  stackedLayout = new QStackedLayout;
  textEditorIndex = stackedLayout->addWidget(textEditor);
  alertEditorIndex = stackedLayout->addWidget(alertEditor);
  QHBoxLayout *switcherLayout = new QHBoxLayout;
  switcherLayout->addWidget(new QLabel(
    tr("At which level do you want to edit this program?")));
  switcherLayout->addWidget(extensionsCombo);
  extensionSwitcher = new QWidget;
  extensionSwitcher->setLayout(switcherLayout);

  compilationError = new QLabel;
  compilationError->setWordWrap(true);
  compilationError->hide();
  QVBoxLayout *l = new QVBoxLayout;
  l->setContentsMargins(QMargins());
  l->addWidget(extensionSwitcher);
  l->addLayout(stackedLayout);
  l->addWidget(compilationError);
  QWidget *w = new QWidget;
  w->setLayout(l);

  editorForm->setCentralWidget(w);
  editorForm->addWidget(editor, true);

  // Connect the error label to this hide/show slot
  connect(&kvs, &KVStore::valueCreated, this, &CodeEdit::setError);
  connect(&kvs, &KVStore::valueChanged, this, &CodeEdit::setError);
}

void CodeEdit::setError(KVPair const &kvp)
{
  if (kvp.first != keyPrefix + "/info") return;
  resetError(&kvp.second);
}

void CodeEdit::resetError(KValue const *kv)
{
  if (kv) {
    std::shared_ptr<conf::SourceInfo const> info =
      std::dynamic_pointer_cast<conf::SourceInfo const>(kv->val);
    if (info) {
      compilationError->setText(stringOfDate(kv->mtime) + ": " + info->errMsg);
      compilationError->setVisible(! info->errMsg.isEmpty());
    } else {
      std::cerr << keyPrefix << "/info is not a SourceInfo?!" << std::endl;
    }
  } else {
    compilationError->setText(tr("Not compiled yet"));
    compilationError->setVisible(true);
  }
}

void CodeEdit::setKeyPrefix(std::string const &prefix)
{
  if (verbose)
    std::cout << "CodeEdit::setKeyPrefix: " << prefix << " replacing "
              << keyPrefix << std::endl;

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
  if (kvs.map.find(ramenKey) != kvs.map.end()) {
    if (verbose)
      std::cout << "CodeEdit::setKeyPrefix: no alert found" << std::endl;
    editor->setCurrentWidget(0);
    editor->setKey(ramenKey);
    stackedLayout->setCurrentIndex(textEditorIndex);
    numSources ++;
  }
  // Takes precedence over the ramen source:
  if (kvs.map.find(alertKey) != kvs.map.end()) {
    if (verbose)
      std::cout << "CodeEdit::setKeyPrefix: found an alert" << std::endl;
    editor->setCurrentWidget(1);
    editor->setKey(alertKey);
    stackedLayout->setCurrentIndex(alertEditorIndex);
    numSources ++;
  }
  extensionSwitcher->setVisible(numSources > 1);
  resetError(kv);
  kvs.lock.unlock_shared();
}
