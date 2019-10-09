#ifndef SERVERINFOWIDGET_H_190923
#define SERVERINFOWIDGET_H_190923
#include <QWidget>

class QFormLayout;
struct KValue;
namespace conf {
  class Value;
};

class ServerInfoWidget : public QWidget
{
  Q_OBJECT

  QFormLayout *layout;

public:
  ServerInfoWidget(QString const &srvUrl, QWidget *parent = nullptr);

protected slots:
  void setKey(std::string const &, KValue const &);
  void setLabel(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
