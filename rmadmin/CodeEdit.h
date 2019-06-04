#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <QTextEdit>
#include "confKey.h"
#include "KValue.h"

class ProgramItem;

class CodeEdit : public QTextEdit
{
  Q_OBJECT

  conf::Key const key;

public:
  CodeEdit(conf::Key const &, QWidget *parent = nullptr);

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const>);
/*  void lockText(conf::Key const &, QString const &uid);
  void unlockText(conf::Key const &);
  void deleteText(conf::Key const &);*/
};

#endif
