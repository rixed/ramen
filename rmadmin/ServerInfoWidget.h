#ifndef SERVERINFOWIDGET_H_190923
#define SERVERINFOWIDGET_H_190923
#include <QWidget>

class QFormLayout;
class KVPair;

class ServerInfoWidget : public QWidget
{
  Q_OBJECT

  QFormLayout *layout;

public:
  ServerInfoWidget(QString const &srvUrl, QWidget *parent = nullptr);

protected slots:
  void setKey(KVPair const &);
};

#endif
