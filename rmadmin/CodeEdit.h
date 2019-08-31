#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <string>
#include <QWidget>
#include "KVPair.h"

class QLabel;
class QStackedLayout;
class QComboBox;
class ProgramItem;
class KTextEdit;
class AtomicForm;
class AlertInfoEditor;
class AtomicWidgetAlternative;

class CodeEdit : public QWidget
{
  Q_OBJECT

public:
  QString const sourceName;
  std::string keyPrefix;

  /* When several source extensions are defined, an additional combo box is
   * visible: */
  QComboBox *extensionsCombo;
  QWidget *extensionSwitcher;

  /* The editor for ramen language sources: */
  KTextEdit *textEditor;
  /* The editor for alert sources: */
  AlertInfoEditor *alertEditor;
  /* The stackedLayout to display either of the above, and their indices: */
  QStackedLayout *stackedLayout;
  /* Finally, the composite AtomicWidget: */
  AtomicWidgetAlternative *editor;
  int textEditorIndex;
  int alertEditorIndex;

  AtomicForm *editorForm;
  QLabel *compilationError;

  CodeEdit(QWidget *parent = nullptr);

protected:
  void resetError(KValue const *);

public slots:
  void setKeyPrefix(std::string const &);

protected slots:
  void setError(KVPair const &);
};

#endif
