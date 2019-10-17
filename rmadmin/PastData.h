#ifndef PASTDATA_H_191007
#define PASTDATA_H_191007

/* This stores past values of a given function.
 * Each request to access past data will lead the server to replay the
 * data for that time range, which is an expensive operation. Also, many times
 * we are interested in a few fields out of many, but we nonetheless request
 * and cache the full tuple for the sake of simplicity.
 *
 * When a time range is requested, only the missing data is requested; boundary
 * timestamp equality will then be used to merge the result into preexisting
 * PastDataChunks as much as possible.
 *
 * Replays must be limited in size as to avoid the backend to read very large
 * amount of data. In theory, this is related to the storage configuration, and
 * if everything is configured properly then no replay should have to process
 * huge amount of data.  In reality, a process to limit the size of the
 * response is needed; ideally controlled solely from the client side. This is
 * still TODO.
 */
#include <memory>
#include <functional>
#include <QObject>
#include "RamenValue.h"
#include "PendingReplayRequest.h"

struct EventTime;
struct RamenType;

class PastData : public QObject
{
  Q_OBJECT

  std::string const site, program, function;

  // List of queries (pending or past!) for this worker, ordered by time:
  std::list<PendingReplayRequest> pendingRequests;

  std::shared_ptr<RamenType const> type;
  std::shared_ptr<EventTime const> eventTime;

public:
  PastData(std::string const &site, std::string const &program,
           std::string const &function,
           std::shared_ptr<RamenType const>,
           std::shared_ptr<EventTime const>,
           QObject *parent = nullptr);

  void request(TimeRange);

  void iterTuples(
    TimeRange, std::function<void (std::shared_ptr<RamenValue const>)> cb) const;
};

#endif
