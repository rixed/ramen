#ifndef TARGETCONFIGEDITOR_H_190611
#define TARGETCONFIGEDITOR_H_190611
#include <memory>
#include <vector>
#include "AtomicWidget.h"

/* An editor for the RC file (or any TargetConfig value).
 *
 * It is also an AtomicWidget.
 * This is mostly a QToolBox of RCEditors. */

class QComboBox;
class QLabel;
class QStackedLayout;
class RCEntryEditor;
namespace conf {
  struct RCEntry;
};

class TargetConfigEditor : public AtomicWidget
{
  Q_OBJECT

  /* So we know where we are coming from when changeEntry is called: */
  int currentIndex;

public:
  QComboBox *entrySelector;
  RCEntryEditor *entryEditor;
  QLabel *noSelectionText;
  QStackedLayout *stackedLayout;
  int entryEditorIdx, noSelectionIdx;
  std::vector<std::shared_ptr<conf::RCEntry>> rcEntries;

  TargetConfigEditor(QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;

  void removeCurrentEntry();

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);

  void preselect(QString const &programName);

protected slots:
  void changeEntry(int idx);
};

#endif
