#include <QVBoxLayout>
#include <QVBoxLayout>
#include <QPushButton>
#include <QLabel>
#include "KTextEdit.h"
#include "AtomicForm.h"
#include "ProgramItem.h"
#include "conf.h"
#include "CodeEdit.h"

CodeEdit::CodeEdit(conf::Key const &keyText_, QWidget *parent) :
  QWidget(parent),
  keyText(keyText_)
{
  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  QString formLabel("Source code for " + QString::fromStdString(keyText.s));
  editorForm = new AtomicForm(formLabel);
  layout->addWidget(editorForm);

  QPushButton *cloneButton = new QPushButton("&Clone");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  textEdit = new KTextEdit(keyText);
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
  editorForm->addWidget(textEdit);

  // Connect the error label to this hide/show slot
  conf::Key const infoKey = conf::changeSourceKeyExt(keyText, "info");
  KValue const *kv = nullptr;
  conf::kvs_lock.lock_shared();
  kv = &conf::kvs[infoKey];
  conf::kvs_lock.unlock_shared();
  connect(kv, &KValue::valueCreated, this, &CodeEdit::setError);
  connect(kv, &KValue::valueChanged, this, &CodeEdit::setError);
  setError(infoKey, kv->val, kv->uid, kv->mtime);
}

void CodeEdit::setError(
  conf::Key const &k,
  std::shared_ptr<conf::Value const> val,
  QString const &, double mtime)
{
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(val);
  if (info) {
    compilationError->setText(stringOfDate(mtime) + ": " + info->errMsg);
    compilationError->setVisible(! info->errMsg.isEmpty());
  } else {
    std::cerr << k << " is not a SourceInfo?!" << std::endl;
  }
}
