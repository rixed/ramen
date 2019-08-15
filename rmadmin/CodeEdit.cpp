#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include "KTextEdit.h"
#include "AtomicForm.h"
#include "ProgramItem.h"
#include "conf.h"
#include "CodeEdit.h"

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

  textEdit = new KTextEdit;
  compilationError = new QLabel;
  compilationError->setWordWrap(true);
  compilationError->hide();
  QVBoxLayout *l = new QVBoxLayout;
  l->setContentsMargins(QMargins());
  l->addWidget(textEdit);
  l->addWidget(compilationError);
  QWidget *w = new QWidget;
  w->setLayout(l);

  editorForm->setCentralWidget(w);
  editorForm->addWidget(textEdit, true);

  // Connect the error label to this hide/show slot
  connect(&kvs, &KVStore::valueCreated, this, &CodeEdit::setError);
  connect(&kvs, &KVStore::valueChanged, this, &CodeEdit::setError);
}

void CodeEdit::setError(KVPair const &kvp)
{
  if (kvp.first != infoKey) return;
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
      std::cerr << infoKey << " is not a SourceInfo?!" << std::endl;
    }
  } else {
    compilationError->setText(tr("Not compiled yet"));
    compilationError->setVisible(true);
  }
}

void CodeEdit::setKey(std::string const &key)
{
  if (key == textKey) return;

  textKey = key;
  infoKey = changeSourceKeyExt(key, "info");

  textEdit->setKey(textKey);

  KValue const *kv = nullptr;

  kvs.lock.lock_shared();
  auto it = kvs.map.find(infoKey);
  if (it != kvs.map.end()) kv = &it->second;
  kvs.lock.unlock_shared();

  resetError(kv);
}
