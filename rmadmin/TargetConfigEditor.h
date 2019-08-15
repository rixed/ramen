#ifndef TARGETCONFIGEDITOR_H_190611
#define TARGETCONFIGEDITOR_H_190611
#include "AtomicWidget.h"

/* An editor for the RC file (or any TargetConfig value).
 *
 * It is also an AtomicWidget.
 * This is mostly a QToolBox of RCEditors. */

class QTabWidget;
class RCEntryEditor;

class TargetConfigEditor : public AtomicWidget
{
  Q_OBJECT

  QTabWidget *rcEntries;

public:
  TargetConfigEditor(QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;

  RCEntryEditor const *currentEntry() const;
  void removeEntry(RCEntryEditor const *);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);

  void preselect(QString const &programName);
};

#endif
