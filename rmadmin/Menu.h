#ifndef MENU_H_190731
#define MENU_H_190731
#include <QObject>

class QMainWindow;
class QMenuBar;
class GraphModel;
class AboutDialog;
class ConfTreeDialog;
class NewSourceDialog;
class NewProgramDialog;
class ProcessesDialog;
class RCEditorDialog;
class GraphModel;

/* We need some slots to open the windows from various places, therefore
 * we need a Q_OBJECT.
 * Of course we only ever want a single instance of it: */

class Menu : public QObject
{
  Q_OBJECT

public:
  GraphModel *graphModel;
  QMenuBar *menuBar;

  AboutDialog *aboutDialog;
  ConfTreeDialog *confTreeDialog;
  NewSourceDialog *newSourceDialog;
  NewProgramDialog *newProgramDialog;
  ProcessesDialog *processesDialog;
  RCEditorDialog *rcEditorDialog;
  // WARNING: The above dialogues must be initialized to null!

  Menu(GraphModel *, bool with_beta_features, QMainWindow * = nullptr);

public slots:
  void openSourceDialog();
  void openNewProgram();
  void openProcesses();
  void openRCEditor();
  void openConfTreeDialog();
  void openAboutDialog();
};

extern Menu *globalMenu;

#endif
