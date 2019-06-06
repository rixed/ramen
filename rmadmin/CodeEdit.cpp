#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QPushButton>
#include <QLabel>
#include "KTextEdit.h"
#include "AtomicForm.h"
#include "ProgramItem.h"
#include "CodeInfoPanel.h"
#include "conf.h"
#include "CodeEdit.h"

CodeEdit::CodeEdit(QString const &sourceName_, QWidget *parent) :
  QWidget(parent),
  sourceName(sourceName_),
  keyText(conf::Key("sources/" + sourceName.toStdString() + "/ramen")),
  keyInfo(conf::Key("sources/" + sourceName.toStdString() + "/info"))
{
  QHBoxLayout *layout = new QHBoxLayout(this);
  setLayout(layout);

  QWidget *placeholder = new CodeInfoPanel(keyInfo);
  layout->addWidget(placeholder);

  QString formLabel("Source code for " + QString::fromStdString(keyText.s));
  editorForm = new AtomicForm(formLabel);
  layout->addWidget(editorForm, 1);

  QPushButton *cloneButton = new QPushButton("&Clone");
  editorForm->buttonsLayout->insertWidget(0, cloneButton);

  textEdit = new KTextEdit(keyText.s);
  editorForm->setCentralWidget(textEdit);
  editorForm->addWidget(textEdit);
}
