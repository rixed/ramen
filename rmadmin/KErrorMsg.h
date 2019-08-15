#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <string>
#include <QLabel>
#include "KVPair.h"

namespace conf {
  class Value;
};

class KErrorMsg : public QLabel
{
  Q_OBJECT

  std::string key;

  void displayError(QString const &);

public:
  KErrorMsg(QWidget *parent = nullptr);

private slots:
  void setValueFromStore(KVPair const &);
  void warnTimeout(KVPair const &);

public slots:
  void setKey(std::string const &);
};

#endif
