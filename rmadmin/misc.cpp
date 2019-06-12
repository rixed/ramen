#include <string>
#include "misc.h"

bool startsWith(std::string const &a, std::string const &b)
{
  if (a.length() < b.length()) return false;
  return 0 == a.compare(0, b.length(), b, 0);
}

bool endsWith(std::string const &a, std::string const &b)
{
  if (a.length() < b.length()) return false;
  return 0 == a.compare(a.length() - b.length(), b.length(), b, 0);
}

std::string const removeExt(std::string const &s)
{
  size_t i = s.rfind('.');
  if (i == std::string::npos) return s;
  return s.substr(0, i);
}

QString const removeExtQ(QString const &s)
{
  int i = s.lastIndexOf('.');
  if (i == -1) return s;
  return s.left(i);
}
