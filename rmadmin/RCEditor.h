#ifndef RCEDITOR_H_190607
#define RCEDITOR_H_190607
#include <QWidget>

/* An editor for a single entry of the target configuration.
 * The actual TargetConfigEditor, bound to the TargetConfig entry in the
 * config tree, will use many of those in a QToolBox. */

class QFormLayout;
class QLabel;

class RCEditor : public QWidget
{
  Q_OBJECT

  QWidget *sourceBox; // either a QComboBox or a mere QLabel
  QLabel *deletedSourceWarning;
  bool sourceDoesExist;

  // Parameters are reset whenever the sourceBox changes:
  QFormLayout *paramsForm;

public:
  QString sourceName;

  // If sourceName is empty then offer to pick one:
  RCEditor(QString const &sourceName, QWidget *parent = nullptr);

  bool isValid() const { return sourceDoesExist; }

public slots:
  /* Refresh the sourceBox with the list of currently existing sources.
   * May be called every time a source (dis)appear but tries to maintain the
   * selection. */
  void resetSources();

  /* Refresh the form each time another source is selected (automatically
   * called by resetSources when needed).
   * Used to reset the parameter table */
  void changedSource();
};

#endif
