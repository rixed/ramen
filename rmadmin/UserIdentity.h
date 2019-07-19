#ifndef USERIDENTITY_H_190719
#define USERIDENTITY_H_190719
#include <QString>
#include <QFile>

struct UserIdentity
{
  bool isValid;
  QString username;
  QString srv_pub_key;
  QString clt_pub_key;
  QString clt_priv_key;

  UserIdentity(QFile &);
};

#endif
