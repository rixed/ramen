#include <algorithm>
#include <optional>
#include <QDebug>
#include "conf.h"
#include "misc.h"

#include "dashboard/tools.h"

std::pair<QString const, std::string const> dashboardNameAndPrefOfKey(
  std::string const &key)
{
  assert(my_socket);

  if (startsWith(key, "dashboards/")) {
    std::string::size_type l = key.rfind('/');
    if (l == std::string::npos || l <= 8) goto err;
    if (0 != key.compare(l - 8, 8, "/widgets")) goto err;
    assert(l > 11 + 8);
    return std::make_pair(QString::fromStdString(key.substr(11, l - 8 - 11)),
                          key.substr(0, l - 8));
  } else if (startsWith(key, "clients/") &&
             0 == key.compare(8, my_socket->size(), *my_socket) &&
             0 == key.compare(8 + my_socket->size(), 19, "/scratchpad/widgets")
  ) {
    return std::make_pair(QString("scratchpad"),
                          key.substr(0, 8 + my_socket->size() + 11));
  }
err:
  return std::make_pair(QString(), std::string());
}

QString const dashboardNameOfKey(std::string const &key)
{
  return dashboardNameAndPrefOfKey(key).first;
}

std::string const dashboardPrefixOfKey(std::string const &key)
{
  return dashboardNameAndPrefOfKey(key).second;
}

void iterDashboards(
  std::function<void(std::string const &, KValue const &,
                     QString const &, std::string const &)> f)
{
  kvs->lock.lock_shared();
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound("e");
  for (std::map<std::string const, KValue>::const_iterator it =
         kvs->map.lower_bound("dashboards/");
       it != end ; it++)
  {
    std::string const &key = it->first;
    KValue const &value = it->second;
    std::pair<QString const, std::string const> name_pref =
      dashboardNameAndPrefOfKey(key);
    if (! name_pref.first.isEmpty())
      f(key, value, name_pref.first, name_pref.second);
  }
  // TODO: Also the users/.../scratchpad keys!
  kvs->lock.unlock_shared();
}

std::optional<int> widgetIndexOfKey(std::string const &key)
{
  std::string const key_prefix = dashboardPrefixOfKey(key);
  if (key_prefix.empty()) return std::nullopt;

  size_t end;
  int const n =
    std::stoi(key.substr(key_prefix.length() + strlen("/widgets/")), &end);
  if (end == 0) return std::nullopt;
  return n;
}

void iterDashboardWidgets(
  std::string const &key_prefix,
  std::function<void(std::string const &, KValue const &, int)> f)
{
  kvs->lock.lock_shared();
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound(key_prefix + "/x");
  std::string const tot_prefix(key_prefix + "/widgets/");
  for (std::map<std::string const, KValue>::const_iterator it =
          kvs->map.lower_bound(tot_prefix);
       it != end ; it++)
  {
    std::string const &key = it->first;
    KValue const &value = it->second;
    if (! startsWith(key, tot_prefix)) continue;
    std::optional<int> const idx = widgetIndexOfKey(key);
    if (idx) f(key, value, *idx);
  }
  // TODO: Also the users/.../scratchpad keys!
  kvs->lock.unlock_shared();
}

int dashboardNumWidgets(std::string const &key_prefix)
{
  int num(0);

  iterDashboardWidgets(key_prefix,
    [&num](std::string const &, KValue const &, int) {
      num++;
  });

  return num;
}

int dashboardNextWidget(std::string const &key_prefix)
{
  int num(-1);

  iterDashboardWidgets(key_prefix,
    [&num](std::string const &, KValue const &, int n) {
      num = std::max(num, n);
  });

  return num + 1;
}
