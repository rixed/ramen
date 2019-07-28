#ifndef USERIDENTITY_H_190719
#define USERIDENTITY_H_190719
#include <optional>
#include <QString>
#include <QFile>
#include "confKey.h"

extern std::optional<QString> my_uid;
extern std::optional<conf::Key> my_errors;

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
