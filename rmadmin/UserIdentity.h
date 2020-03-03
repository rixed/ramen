#ifndef USERIDENTITY_H_190719
#define USERIDENTITY_H_190719
#include <optional>
#include <QFile>
#include <QString>

extern std::optional<QString> my_uid;
extern std::optional<std::string> my_errors;
extern std::optional<std::string> my_socket;

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
