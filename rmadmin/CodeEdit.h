#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <memory>
#include <string>
#include <QWidget>
#include "conf.h"

class AlertInfoEditor;
class AtomicWidget;
class KTextEdit;
struct KValue;
class ProgramItem;
class QComboBox;
class QLabel;
class QStackedLayout;
class SourceInfoViewer;

namespace conf {
  class Value;
};

class CodeEdit : public QWidget
{
  Q_OBJECT

  AtomicWidget const *currentWidget() const;

  void setError(std::string const &, KValue const &);
  void setLanguageKey(int, AtomicWidget *, std::string const &);

public:
  std::string keyPrefix;

  /* When several source extensions are defined, an additional combo box is
   * visible: */
  QComboBox *extensionsCombo;
  QWidget *extensionSwitcher;

  /* The editor for alert sources: */
  AlertInfoEditor *alertEditor;
  /* The editor for ramen language sources: */
  KTextEdit *textEditor;
  /* We do not really edit the info but that's nice to see it here, and it is
   * more natural that the form lock/unlock/delete the info key alongside
   * the other editable sources. Especially, the deletion of the info file is
   * what triggers the worker to be stopped. */
  SourceInfoViewer *infoEditor;

  /* The stackedLayout to display either of the above, and their indices: */
  QStackedLayout *stackedLayout;
  int textEditorIndex;
  int alertEditorIndex;
  int infoEditorIndex;

  QLabel *compilationError;

  CodeEdit(QWidget *parent = nullptr);

  // Returns the value of the value of the currently selected editor:
  std::shared_ptr<conf::Value const> getValue() const;

  void enableLanguage(int index, bool enabled);

  bool hasValidInput() const;

protected:
  void resetError(KValue const *);
  void doResetError(KValue const &);

public slots:
  void setKeyPrefix(std::string const &);
  /* Display the editor corresponding to the given language index (either
   * textEditorIndex or alertEditorIndex): */
  void setLanguage(int index);
  void disableLanguageSwitch(bool);

protected slots:
  void onChange(QList<ConfChange> const &);

signals:
  void inputChanged();
};

#endif
