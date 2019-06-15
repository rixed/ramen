#ifndef RCENTRYEDITOR_H_190607
#define RCENTRYEDITOR_H_190607
#include <QWidget>

/* An editor for a single entry of the target configuration.
 * The actual TargetConfigEditor, bound to the TargetConfig entry in the
 * config tree, will use many of those in a QToolBox. */

class QFormLayout;
class QLabel;
class QComboBox;
class QLineEdit;
class QCheckBox;
namespace conf {
  struct RCEntry;
};

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

  // Parameters are reset whenever the sourceBox changes:
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

  // Caller takes ownership
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

  /* Set the form values according to this RCEntry: */
  void setValue(conf::RCEntry const *);
};

#endif
