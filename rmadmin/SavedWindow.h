#ifndef SAVEDWINDOW_H_190813
#define SAVEDWINDOW_H_190813
#include <QMainWindow>
#include <QString>

/* Like QMainWindow but with some adjustments, such as saving/restoring
 * the window position, thus the name. */

class QCloseEvent;

class SavedWindow : public QMainWindow
{
  Q_OBJECT

  QString windowName;

public:
  SavedWindow(
    QString const &windowName, QString const &windowTitle,
    QWidget *parent = nullptr);

protected:
  void closeEvent(QCloseEvent *);
};

#endif
