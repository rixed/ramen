#ifndef MENU_H_190731
#define MENU_H_190731
#include <QObject>

class QMainWindow;
class QMenuBar;
class AboutDialog;
class ConfTreeDialog;
class NewSourceDialog;
class NewProgramDialog;
class ProcessesDialog;
class RCEditorDialog;

/* We need some slots to open the windows from various places, therefore
 * we need a Q_OBJECT.
 * Of course we only ever want a single instance of it: */

class Menu : public QObject
{
  Q_OBJECT

public:
  QMenuBar *menuBar;

  static AboutDialog *aboutDialog;
  static ConfTreeDialog *confTreeDialog;
  static NewSourceDialog *newSourceDialog;
  static NewProgramDialog *newProgramDialog;
  static ProcessesDialog *processesDialog;
  static RCEditorDialog *rcEditorDialog;

  Menu(bool with_beta_features, QMainWindow *);

public slots:
  void openSourceDialog();
  void openNewProgram();
  void openProcesses();
  void openRCEditor();
  void openConfTreeDialog();
  void openAboutDialog();
};

#endif
