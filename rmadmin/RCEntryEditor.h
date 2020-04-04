#ifndef RCENTRYEDITOR_H_190607
#define RCENTRYEDITOR_H_190607
#include <memory>
#include <string>
#include <QWidget>
#include <QMap>
#include <QCheckBox>
#include "conf.h"

/* An editor for a single entry of the target configuration.
 * The actual TargetConfigEditor, bound to the TargetConfig entry in the
 * config tree, will use many of those in a QToolBox. */

struct CompiledProgramParam;
struct KValue;
class QFormLayout;
class QLabel;
class QComboBox;
class QLineEdit;
struct RamenValue;
namespace conf {
  class Key;
  struct RCEntry;
};

class RCEntryEditor : public QWidget
{
  Q_OBJECT

  friend class TargetConfigEditor;

protected:
  QLineEdit *suffixEdit;
  QComboBox *sourceBox;
  QLabel *deletedSourceWarning;
  QLabel *notCompiledSourceWarning;
  bool sourceDoesExist;
  bool sourceIsCompiled;
  QCheckBox *enabledBox, *debugBox, *automaticBox;
  QLineEdit *sitesEdit;
  QLineEdit *reportEdit;
  QLineEdit *cwdEdit;

  /* The SourceInfo defines the possible parameters (as CompiledProgramParam
   * objects), with a name, a doc and a default value. On top of that, the
   * conf::RCEntry comes with a set of conf::RCEntryParams overwriting the
   * defaults.
   * The form must offer to edit every defined param for that source, taking
   * values from all defined RCEntryParams and then CompiledProgramParam.
   * When the sourceBox is changed, the set of defined parameters change,
   * yet we keep the former values for the RCEntryParams, so no value is
   * ever lost by changing this combobox.
   * When computing the value of the RCEntryEditor, though, we take only the
   * parameters that are defined in the selected source. */
  // Returned value still owned by the callee
  std::shared_ptr<RamenValue const> paramValue(
    std::shared_ptr<CompiledProgramParam const>) const;

  // Bag of previously set parameter values:
  static QMap<std::string, std::shared_ptr<RamenValue const>> setParamValues;

  /* Keep the layout so it can be reset and also the widget and param
   * names can be retrieved: */
  QFormLayout *paramsForm;

  /* Whether the edition of this RC entry is currently enabled or not: */
  bool enabled;

  void addSourceFromStore(std::string const &, KValue const &);
  void updateSourceFromStore(std::string const &, KValue const &);
  void removeSourceFromStore(std::string const &, KValue const &);

public:
  bool sourceEditable;

  RCEntryEditor(bool sourceEditable = true, QWidget *parent = nullptr);

  // Select that one, even if it does not exist:
  void setProgramName(std::string const &);

  /* An RCEntryEditor can be used to edit an existing entry when editing the
   * TargetConfig, or to create a new entry when asking to run a new program.
   * In the former case, the editor must be enabled/disabled according to
   * the TargetConfig lock state in the confserver, using this setEnabled
   * function. In the later case, the RCEntryEditor must be enabled once
   * after creation, as its initially constructed disabled: */
  void setEnabled(bool);

  /* Both addSource and findOrAddSourceName return the position in the select
   * box where the name has been inserted/found (so it can be programmatically
   * selected), or -1 if it has not been inserted. */
  int addSource(std::string const &);
  int findOrAddSourceName(QString const &);

  void updateSourceWarnings();

  void saveParams();
  void clearParams();

  bool isValid() const;

  /* Build a new RCEntry according to current content.
   * Caller takes ownership */
  conf::RCEntry *getValue() const;

  bool programIsEnabled() const { return enabledBox && enabledBox->isChecked(); }

signals:
  void inputChanged();

private slots:
  void onChange(QList<ConfChange> const &);

private:
  /* Refresh the params each time another source is selected.
   * Used to reset the parameter table */
  void resetParams();
  // Set the form values according to this RCEntry:
  void setValue(conf::RCEntry const &);
};

#endif
