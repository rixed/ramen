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
  QVBoxLayout *layout = new QVBoxLayout(this);
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  QString formLabel("Source code for " + QString::fromStdString(keyText.s));
  editorForm = new AtomicForm(formLabel);
  layout->addWidget(editorForm);

  QPushButton *cloneButton = new QPushButton("&Clone");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  textEdit = new KTextEdit(keyText);
  editorForm->setCentralWidget(textEdit);
  editorForm->addWidget(textEdit);
}
