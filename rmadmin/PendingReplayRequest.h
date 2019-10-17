#ifndef PENDINGREPLAYREQUEST_H_191007
#define PENDINGREPLAYREQUEST_H_191007
#include <ctime>
#include <memory>
#include <QObject>
#include "TimeRange.h"

struct EventTime;
struct KValue;
struct RamenType;
struct RamenValue;

class PendingReplayRequest : public QObject
{
  Q_OBJECT

  std::time_t started; // When the query was sent (for timeout)
  std::string const respKey; // Used to identify a single request

  bool completed;
  std::shared_ptr<RamenType const> type;
  std::shared_ptr<EventTime const> eventTime;

public:
  TimeRange timeRange;

  /* Where the results are stored (in event time order once completed, or
   * random): */
  std::vector<std::pair<double, std::shared_ptr<RamenValue const>>> tuples;

  /* Also start the actual request: */
  PendingReplayRequest(
    std::string const &site, std::string const &program,
    std::string const &function, TimeRange range,
    std::shared_ptr<RamenType const> type_,
    std::shared_ptr<EventTime const>);

protected slots:
  void receiveValue(std::string const &, KValue const &);
  void endReceived();
};

#endif
