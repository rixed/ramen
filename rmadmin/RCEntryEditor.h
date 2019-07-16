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
  struct RCEntry;
};
struct CompiledProgramParam;

class RCEntryEditor : public QWidget
{
  Q_OBJECT

  QLineEdit *nameEdit;
  QComboBox *sourceBox;
  QLabel *deletedSourceWarning;
  bool sourceDoesExist;
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
  QMap<std::string, std::shared_ptr<RamenValue const>> setParamValues;

  /* Keep the layout so it can be reset and also the widget and param
   * names can be retrieved: */
  QFormLayout *paramsForm;

public:
  QString sourceName;
  bool sourceEditable;

  // Provision and preselect sourceName
  RCEntryEditor(QString const &sourceName, bool sourceEditable, QWidget *parent = nullptr);
  RCEntryEditor(conf::RCEntry const *, QWidget *parent = nullptr);

  void setSourceExists(bool);

  void clearParams();

  bool isValid() const { return sourceDoesExist; }

  /* Build a new RCEntry according to current content.
   * Caller takes ownership */
  conf::RCEntry *getValue() const;

public slots:
  /* Refresh the sourceBox with the list of currently existing sources.
   * May be called every time a source (dis)appear but tries to maintain the
   * selection. */
  void resetSources();

  /* Refresh the params each time another source is selected (automatically
   * called by resetSources when needed).
   * Used to reset the parameter table */
  void resetParams();

  // Set the form values according to this RCEntry:
  void setValue(conf::RCEntry const *);
};

#endif
