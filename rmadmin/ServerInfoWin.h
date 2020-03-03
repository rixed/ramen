#ifndef SERVERINFOWIN_H_190923
#define SERVERINFOWIN_H_190923
#include "SavedWindow.h"

class QString;
class QWidget;

class ServerInfoWin : public SavedWindow
{
  Q_OBJECT

public:
  ServerInfoWin(QString const &srvUrl, QWidget *parent = nullptr);
};

#endif
