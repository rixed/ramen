#ifndef RCENTRYEDITOR_H_190607
#define RCENTRYEDITOR_H_190607
#include <memory>
#include <string>
#include <QWidget>
#include <QMap>

/* An editor for a single entry of the target configuration.
 * The actual TargetConfigEditor, bound to the TargetConfig entry in the
 * config tree, will use many of those in a QToolBox. */

class QFormLayout;
class QLabel;
class QComboBox;
class QLineEdit;
class QCheckBox;
struct RamenValue;
namespace conf {
  class Key;
  struct RCEntry;
};
struct CompiledProgramParam;

class RCEntryEditor : public QWidget
{
  Q_OBJECT

  QLineEdit *nameEdit;
  QComboBox *sourceBox;
  QLabel *deletedSourceWarning;
  QLabel *notCompiledSourceWarning;
  bool sourceDoesExist;
  bool sourceIsCompiled;
  QCheckBox *enabledBox, *debugBox, *automaticBox;
  QLineEdit *sitesEdit;
  QLineEdit *reportEdit;

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
  std::shared_ptr<RamenValue const> paramValue(CompiledProgramParam const *) const;

  // Bag of previously set parameter values:
  static QMap<std::string, std::shared_ptr<RamenValue const>> setParamValues;

  /* Keep the layout so it can be reset and also the widget and param
   * names can be retrieved: */
  QFormLayout *paramsForm;

public:
  bool sourceEditable;

  RCEntryEditor(bool sourceEditable = true, QWidget *parent = nullptr);

  // Select that one, even if it does not exist:
  void setSourceName(QString const &);

  void addSource(conf::Key const &);
  void addSourceName(QString const &);
  void updateSourceWarnings();

  void clearParams();

  bool isValid() const { return sourceDoesExist && sourceIsCompiled; }

  /* Build a new RCEntry according to current content.
   * Caller takes ownership */
  conf::RCEntry *getValue() const;

public slots:
  /* Refresh the params each time another source is selected.
   * Used to reset the parameter table */
  void resetParams();

  // Set the form values according to this RCEntry:
  void setValue(conf::RCEntry const *);
};

#endif
