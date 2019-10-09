#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <memory>
#include <string>
#include <QWidget>

class AlertInfoEditor;
class AtomicWidgetAlternative;
class KTextEdit;
struct KValue;
class ProgramItem;
class QLabel;
class QStackedLayout;
class QComboBox;
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
  void doResetError(KValue const &);

public slots:
  void setKeyPrefix(std::string const &);
  /* Display the editor corresponding to the given language index (either
   * textEditorIndex or alertEditorIndex): */
  void setLanguage(int index);

protected slots:
  void setError(std::string const &, KValue const &);
};

#endif
