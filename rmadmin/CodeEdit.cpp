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
#include "CloneDialog.h"
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

  QPushButton *cloneButton = new QPushButton("&Clone…");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  textEditor = new KTextEdit;
  alertEditor = new AlertInfoEditor;
  editor = new AtomicWidgetAlternative;
  editor->addWidget(textEditor);
  editor->addWidget(alertEditor);
  // TODO: set the actually available options in setKeyPrefix:
  extensionsCombo = new QComboBox;
  stackedLayout = new QStackedLayout;
  /* Beware: Same indices are used to access both stackedLayout and
   * extensionsCombo: */
  textEditorIndex = stackedLayout->addWidget(textEditor);
  extensionsCombo->addItem("ramen");
  alertEditorIndex = stackedLayout->addWidget(alertEditor);
  extensionsCombo->addItem("alert");
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

  // Connect the clone button to the creation of a cloning dialog:
  connect(cloneButton, &QPushButton::clicked,
          this, &CodeEdit::wantClone);
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
    extensionsCombo->setCurrentIndex(textEditorIndex);
    numSources ++;
  }
  // Takes precedence over the ramen source:
  if (kvs.map.find(alertKey) != kvs.map.end()) {
    if (verbose)
      std::cout << "CodeEdit::setKeyPrefix: found an alert" << std::endl;
    editor->setCurrentWidget(1);
    editor->setKey(alertKey);
    stackedLayout->setCurrentIndex(alertEditorIndex);
    extensionsCombo->setCurrentIndex(alertEditorIndex);
    numSources ++;
  }
  extensionSwitcher->setVisible(numSources > 1);
  resetError(kv);
  kvs.lock.unlock_shared();
}

void CodeEdit::wantClone()
{
  if (verbose)
    std::cout << "CodeEdit::wantClone: keyPrefix=" << keyPrefix
              << ", extension=" << extensionsCombo->currentText().toStdString()
              << std::endl;

  if (keyPrefix.empty()) return;

  /* We might want to have as many of those dialogs open as we want
   * to create clones (possibly of the same source); So just create
   * a new dialog each time the clone button is clicked.
   * Note that we clone only the selected extension. */
  std::string orig = keyPrefix +"/" + extensionsCombo->currentText().toStdString();
  CloneDialog *dialog = new CloneDialog(orig, this);
  dialog->show();
}
