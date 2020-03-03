#ifndef REPLAYREQUEST_H_191007
#define REPLAYREQUEST_H_191007
/* A ReplayRequest is a set of tuples obtained form the server and stored to
 * feed the charts and tail tables. */
#include <ctime>
#include <memory>
#include <string>
#include <QObject>

struct EventTime;
struct KValue;
struct RamenType;
struct RamenValue;

extern std::string const respKeyPrefix;

class ReplayRequest : public QObject
{
  Q_OBJECT

  std::time_t started; // When the query was sent (for timeout)
  std::string const respKey; // Used to identify a single request

  bool completed;
  std::shared_ptr<RamenType const> type;
  std::shared_ptr<EventTime const> eventTime;

public:
  double since, until;

  /* Where the results are stored (in event time order once completed, or
   * random): */
  std::vector<std::pair<double, std::shared_ptr<RamenValue const>>> tuples;

  /* Also start the actual request: */
  ReplayRequest(
    std::string const &site, std::string const &program,
    std::string const &function, double since_, double until_,
    std::shared_ptr<RamenType const> type_,
    std::shared_ptr<EventTime const>);

protected slots:
  void receiveValue(std::string const &, KValue const &);
  void endReceived();

// TODO: a signal sent when completed or when new tuples have been added (with
// a QTimer)
};

#endif
