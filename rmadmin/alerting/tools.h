#ifndef TOOLS_H_200525
#define TOOLS_H_200525
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

struct VString;
namespace conf {
  class Value;
  struct IncidentLog;
  struct Notification;
};

void iterTeams(
  std::function<void(std::string const &)>);

void iterIncidents(
  std::function<void(std::string const &)>);

void iterDialogs(
  std::string const &incidentId,
  std::function<void(std::string const &)>);

// In no particular order:
void iterLogs(
  std::string const &incidentId,
  std::function<void(double,
                     std::shared_ptr<conf::IncidentLog const>)>);

bool parseLogKey(
  std::string const &k,
  std::string *incidentId,
  double *time);

std::string const incidentKey(
  std::string const &incidentId,
  std::string const &k);

std::shared_ptr<conf::Value const> getIncident(
  std::string const &incidentId,
  std::string const &k);

std::shared_ptr<conf::Notification const> getIncidentNotif(
  std::string const &incidentId,
  std::string const &k);

std::shared_ptr<VString const> getAssignedTeam(std::string const &incidentId);

std::string const dialogKey(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k);

std::shared_ptr<conf::Value const> getDialog(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k);

/* Same as above but returns a float directly: */
std::optional<double> getDialogDate(
  std::string const &incidentId,
  std::string const &dialogId,
  std::string const &k);

#endif
