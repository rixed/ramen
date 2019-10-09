#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <string>
#include <QLabel>

struct KValue;
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
  void setValueFromStore(std::string const &, KValue const &);
  void warnTimeout(std::string const &, KValue const &);

public slots:
  void setKey(std::string const &);
};

#endif
