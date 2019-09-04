#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <memory>
#include <string>
#include <QWidget>
#include "KVPair.h"

class QLabel;
class QStackedLayout;
class QComboBox;
class ProgramItem;
class KTextEdit;
class AlertInfoEditor;
class AtomicWidgetAlternative;
namespace conf {
  class Value;
};

// FIXME: inherit AtomicWidgetAlternative?
class CodeEdit : public QWidget
{
  Q_OBJECT

public:
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

  QLabel *compilationError;

  CodeEdit(QWidget *parent = nullptr);

protected:
  void resetError(KValue const *);

public slots:
  void setKeyPrefix(std::string const &);
  /* Display the editor corresponding to the given language index (either
   * textEditorIndex or alertEditorIndex): */
  void setLanguage(int index);

protected slots:
  void setError(KVPair const &);
};

#endif
