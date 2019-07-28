#include <iostream>
#include <string>
#include <QJsonDocument>
#include <QJsonObject>
#include "UserIdentity.h"

std::optional<QString> my_uid;
std::optional<conf::Key> my_errors;

static void complain(QFile &file, std::string msg)
{
  std::cerr << "File " << file.fileName().toStdString() << ": "
            << msg << std::endl;
}

UserIdentity::UserIdentity(QFile &file)
{
  isValid = false;
  do {
    if (! file.open(QIODevice::ReadOnly | QIODevice::Text)) {
      complain(file, "Cannot open");
      break;
    }

    QByteArray txt = file.readAll();
    file.close();

    QJsonParseError error;
    QJsonDocument doc = QJsonDocument::fromJson(txt, &error);
    if (doc.isNull()) {
      complain(file, error.errorString().toStdString());
      break;
    }
    if (!doc.isObject()) {
      complain(file, "Cannot parse as JSON");
      break;
    }

    QJsonObject obj = doc.object();

#   define GET(field, name) \
    if (! obj.contains(name) || ! obj[name].isString()) { \
      complain(file, "Cannot find/parse " name); \
      break; \
    } \
    field = obj[name].toString();

    GET(username, "username");
    GET(srv_pub_key, "server_public_key");
    GET(clt_pub_key, "client_public_key");
    GET(clt_priv_key, "client_private_key");

    isValid = true;
  } while (false);
}
