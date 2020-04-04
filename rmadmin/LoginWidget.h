#ifndef LOGINWIDGET_H_191107
#define LOGINWIDGET_H_191107
/* Widget asking for all connection parameters:
 * - user name (combo of recently used ones + unix default + a blank
 *   editable entry),
 * - if the connection is secure, identity file (select from past ones
 *   or open a file picker in the default config location),
 * - a submit button which label can then be changed into a status.
 * There is also a Cancel button (that will quits the app). */
#include <QWidget>
#include <QString>

class QComboBox;
class QLabel;
class QPushButton;
class QRadioButton;
class QStackedLayout;
struct UserIdentity;

class LoginWidget : public QWidget
{
  Q_OBJECT

  QString const configDir;

  QComboBox *serverCombo;
  QComboBox *usernameCombo;
  QRadioButton *insecureCheck, *secureCheck;
  QComboBox *idFileCombo;
  QPushButton *pickFileButton, *submitButton, *cancelButton;
  QLabel *connectStatus;
  QStackedLayout *buttonsOrStatus;
  int buttonsIdx, statusIdx;

  void resizeFileCombo();

public:
  LoginWidget(QString const configDir, QWidget *parent = nullptr);

  void focusSubmit();

  QSize sizeHint() const { return QSize(500, 300); }

protected slots:
  void setSecure(bool);
  void pickNewFile();
  void submitLogin();

public slots:
  // Disable and hide the submit button and show instead the given text:
  void setSubmitStatus(QString const);

signals:
  void submitted(QString const server, QString const username,
                 QString const idFile);
  void cancelled();
};

#endif
