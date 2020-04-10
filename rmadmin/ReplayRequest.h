#ifndef REPLAYREQUEST_H_191007
#define REPLAYREQUEST_H_191007
/* A ReplayRequest is a set of tuples obtained form the server and stored to
 * feed the charts and tail tables. */
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <QObject>
#include "conf.h"

struct EventTime;
class QTimer;
struct KValue;
struct RamenType;
struct RamenValue;

extern std::string const respKeyPrefix;

class ReplayRequest : public QObject
{
  Q_OBJECT

  QTimer *timer;

  std::string const site, program, function;

  void receiveValue(std::string const &, KValue const &);
  void endReplay(std::string const &, KValue const &);

public:
  // Protects status, since, until and tuples:
  std::mutex lock;

  std::time_t started; // When the query was sent (for timeout)
  std::string const respKey; // Used to identify a single request

  std::shared_ptr<RamenType const> type;
  std::shared_ptr<EventTime const> eventTime;

  enum Status { Waiting, Sent, Completed } status;

  static QString const qstringOfStatus(ReplayRequest::Status const);

  double since, until;

  /* Where the results are stored (in event time order. */
  std::multimap<double, std::shared_ptr<RamenValue const>> tuples;

  /* Also start the actual request: */
  ReplayRequest(
    std::string const &site, std::string const &program,
    std::string const &function, double since_, double until_,
    std::shared_ptr<RamenType const> type_,
    std::shared_ptr<EventTime const>,
    QObject *parent = nullptr);

  bool isCompleted(std::lock_guard<std::mutex> const &) const;
  bool isWaiting(std::lock_guard<std::mutex> const &) const;

protected slots:
  void sendRequest();
  void onChange(QList<ConfChange> const &);

signals:
  void tupleBatchReceived();
  void endReceived();
};

#endif
