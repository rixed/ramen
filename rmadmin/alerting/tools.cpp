#include <set>
#include <utility>
#include <QDebug>

#include "conf.h"
#include "confValue.h"
#include "RamenValue.h"
#include "alerting/tools.h"

static bool const verbose { false };

std::string const incidentKey(std::string const &incidentId, std::string const &k)
{
  return "alerting/incidents/" + incidentId + "/" + k;
}

std::shared_ptr<conf::Value const> getIncident(
  std::string const &incidentId, std::string const &k)
{
  return kvs->get(incidentKey(incidentId, k));
}

std::shared_ptr<conf::Notification const> getIncidentNotif(
  std::string const &incidentId,
  std::string const &k)
{
  std::shared_ptr<conf::Notification const> notif {
    std::dynamic_pointer_cast<conf::Notification const>(
      getIncident(incidentId, k)) };
  return notif;
}

std::shared_ptr<VString const> getAssignedTeam(std::string const &incidentId)
{
  std::shared_ptr<conf::RamenValueValue const> ramenValue {
    std::dynamic_pointer_cast<conf::RamenValueValue const>(
      getIncident(incidentId, "team")) };
  if (! ramenValue) return nullptr;
  return std::dynamic_pointer_cast<VString const>(ramenValue->v);
}

void iterTeams(
  std::function<void(std::string const &)> f)
{
  kvs->lock.lock_shared();
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound("alerting/teamt");
  std::set<std::string> names;
  for (std::map<std::string const, KValue>::const_iterator it =
         kvs->map.lower_bound("alerting/teams/");
       it != end ; it++)
  {
    std::string const &key { it->first };
    size_t const end { key.find('/', 15) };
    std::string const name { key.substr(15, end - 15) };
    auto it_new { names.insert(name) };
    if (it_new.second) {
      if (verbose)
        qDebug() << "New teamName:" << QString::fromStdString(name);
      f(name);
    }
  }
  kvs->lock.unlock_shared();
}

void iterIncidents(
  std::function<void(std::string const &)> f)
{
  kvs->lock.lock_shared();
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound("alerting/incidentt");
  std::set<std::string> ids;
  for (std::map<std::string const, KValue>::const_iterator it =
         kvs->map.lower_bound("alerting/incidents/");
       it != end ; it++)
  {
    std::string const &key { it->first };
    size_t const end { key.find('/', 19) };
    std::string const incidentId { key.substr(19, end - 19) };
    auto it_new { ids.insert(incidentId) };
    if (it_new.second) {
      if (verbose)
        qDebug() << "New incidentId:" << QString::fromStdString(incidentId);
      f(incidentId);
    }
  }
  kvs->lock.unlock_shared();
}

void iterDialogs(
  std::string const &incidentId,
  std::function<void(std::string const &)> f)
{
  kvs->lock.lock_shared();
  std::string const startKey { incidentKey(incidentId, "dialogs/") };
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound(incidentKey(incidentId, "dialogt"));
  std::set<std::string> ids;
  for (std::map<std::string const, KValue>::const_iterator it =
         kvs->map.lower_bound(startKey) ;
       it != end ; it++)
  {
    std::string const &key { it->first };
    size_t const prefLen { startKey.length() };
    size_t const end { key.find('/', prefLen) };
    std::string const dialogId { key.substr(prefLen, end - prefLen) };
    auto it_new { ids.insert(dialogId) };
    if (it_new.second) {
      if (verbose)
        qDebug() << "New dialogId:" << QString::fromStdString(dialogId);
      f(dialogId);
    }
  }
  kvs->lock.unlock_shared();
}

bool parseLogKey(
  std::string const &k,
  std::string *incidentId,
  double *time)
{
  if (! startsWith(k, "alerting/incidents/")) return false;

  if (verbose)
    qDebug() << "parseLogKey: key is" << QString::fromStdString(k);

  // alerting/incidents/619ea496-d54a-40f8-8df0-6f5cd3411c50/journal/0x1.7b39563c5661bp+30/289771782
  size_t e1 { k.find('/', 19) };
  if (e1 == std::string::npos) return false;
  if (incidentId) {
    *incidentId = k.substr(19, e1 - 19);
    if (verbose)
      qDebug() << "parseLogKey: Found incidentId"
               << QString::fromStdString(*incidentId);
  }
  if (0 != k.compare(e1 + 1, 8, "journal/")) return false;
  e1 += 9;
  size_t const e2 { k.find('/', e1) };
  if (e2 == std::string::npos) return false;

  std::string const str { k.substr(e1, e2 - e1) };
  if (verbose)
    qDebug() << "parseLogKey: time is substring" << QString::fromStdString(str);

  char *end;
  double t { std::strtod(str.c_str(), &end) };
  if (*end != '\0') return false;

  if (time) {
    *time = t;
    if (verbose)
      qDebug() << "parseLogKey: Found time" << t;
  }

  return true;
}

void iterLogs(
  std::string const &incidentId,
  std::function<void(double,
                     std::shared_ptr<conf::IncidentLog const>)> f)
{
  kvs->lock.lock_shared();
  std::string const startKey { incidentKey(incidentId, "journal/") };
  std::map<std::string const, KValue>::const_iterator const end =
    kvs->map.upper_bound(incidentKey(incidentId, "journam"));
  std::set<std::string> ids;
  for (std::map<std::string const, KValue>::const_iterator it =
         kvs->map.lower_bound(startKey) ;
       it != end ; it++)
  {
    std::string const &key { it->first };
    double time;
    if (! parseLogKey(key, nullptr, &time)) continue;
    std::shared_ptr<conf::IncidentLog const> log {
      std::dynamic_pointer_cast<conf::IncidentLog const>(it->second.val) };
    if (! log) continue;

    if (verbose)
      qDebug() << "New log for time:" << time;

    f(time, log);
  }
  kvs->lock.unlock_shared();
}

std::string const dialogKey(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k)
{
  return "alerting/incidents/" + incidentId + "/dialogs/" + dialogId + "/" + k;
}

std::shared_ptr<conf::Value const> getDialog(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k)
{
  return kvs->get(dialogKey(incidentId, dialogId, k));
}

std::optional<double> getDialogDate(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k)
{
  std::shared_ptr<conf::RamenValueValue const> dataValue {
    std::dynamic_pointer_cast<conf::RamenValueValue const>(
      getDialog(incidentId, dialogId, k)) };
  if (! dataValue) return std::nullopt;

  std::shared_ptr<VFloat const> floatValue {
    std::dynamic_pointer_cast<VFloat const>(dataValue->v) };
  if (! floatValue) return std::nullopt;

  return floatValue->v;
}
