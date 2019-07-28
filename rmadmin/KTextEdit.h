#ifndef KTEXTEDIT_H_190603
#define KTEXTEDIT_H_190603
#include <QTextEdit>
#include "KValue.h"
#include "AtomicWidget.h"

class KTextEdit : public AtomicWidget
{
  Q_OBJECT

  QTextEdit *textEdit;

public:
  KTextEdit(QString const &sourceName, QWidget *parent = nullptr);

  virtual std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const> v);
  void lockValue(conf::Key const &k, QString const &uid)
  {
    AtomicWidget::lockValue(k, uid);
  }
  void unlockValue(conf::Key const &k)
  {
    AtomicWidget::unlockValue(k);
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
