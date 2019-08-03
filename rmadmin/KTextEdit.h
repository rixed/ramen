#ifndef KTEXTEDIT_H_190603
#define KTEXTEDIT_H_190603
#include <QTextEdit>
#include "KValue.h"
#include "AtomicWidget.h"

class KTextEdit : public AtomicWidget
{
  Q_OBJECT

  QTextEdit *textEdit;

  void extraConnections(KValue *);

public:
  KTextEdit(QWidget *parent = nullptr);

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const> v);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
