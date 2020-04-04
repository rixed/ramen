#ifndef SERVERINFOWIDGET_H_190923
#define SERVERINFOWIDGET_H_190923
#include <memory>
#include <QWidget>
#include "conf.h"

class QFormLayout;
struct KValue;
namespace conf {
  class Value;
};

class ServerInfoWidget : public QWidget
{
  Q_OBJECT

  QFormLayout *layout;

  void setKey(std::string const &, KValue const &);
  void setLabel(std::string const &, std::shared_ptr<conf::Value const>);

public:
  ServerInfoWidget(QString const &srvUrl, QWidget *parent = nullptr);

protected slots:
  void onChange(QList<ConfChange> const &);
};

#endif
