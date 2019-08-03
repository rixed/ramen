#ifndef TARGETCONFIGEDITOR_H_190611
#define TARGETCONFIGEDITOR_H_190611
#include <QToolBox>
#include "AtomicWidget.h"

/* An editor for the RC file (or any TargetConfig value).
 *
 * It is also an AtomicWidget.
 * This is mostly a QToolBox of RCEditors. */

class TargetConfigEditor : public AtomicWidget
{
  Q_OBJECT

  QToolBox *toolBox;

  void extraConnections(KValue *);

public:
  TargetConfigEditor(QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

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
