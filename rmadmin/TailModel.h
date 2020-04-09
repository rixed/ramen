#ifndef TAILMODEL_H_190515
#define TAILMODEL_H_190515
#include <cmath>
#include <map>
#include <memory>
#include <vector>
#include <QAbstractItemModel>
#include <QString>
#include <QStringList>
#include "conf.h"

/* The model representing lines of tuples, with possibly some tuples skipped
 * in between 2 lines. The model stores *all* tuples and is owned by a
 * function, that share it with 0 or several widgets. When the function is
 * the only user then it can, after a while, destroy it to reclaim memory.
 * The function will also delete its counted reference to the TailModel
 * whenever the worker change.
 *
 * All of this happen behind TailModel's back though, as the TailModel itself
 * is only given the identifier (site/fq/instance) it must subscribe to (and
 * unsubscribe at destruction), and an unserializing function (or rather, the
 * tuple RamenType).
 *
 * It then receives and stores the tuples, as a pair of event time (for those
 * tuples that have one, or 0) and the unserialized RamenValues.
 */

struct EventTime;
struct KValue;
struct RamenValue;
struct RamenType;
namespace conf {
  class Value;
};

class TailModel : public QAbstractTableModel
{
  Q_OBJECT

  std::shared_ptr<EventTime const> eventTime;

  double minEventTime_ = NAN;
  double maxEventTime_ = NAN;

  void addTuple(std::string const &, KValue const &);

public:
  QString const fqName;
  QString workerSign;
  std::string const keyPrefix;

  /* Unlike for Replayrequests, for tails we actually want to know in which orders
   * are the tuples emitted (plus, they could have no event-time at all).
   * Therefore we keep both a vector and a multimap: */
  std::vector<std::pair<double, std::shared_ptr<RamenValue const>>> tuples;
  std::multimap<double, size_t> order;

  std::shared_ptr<RamenType const> type;

  TailModel(
    QString const &fqName, QString const &workerSign,
    std::shared_ptr<RamenType const> type,
    std::shared_ptr<EventTime const>,
    QObject *parent = nullptr);

  ~TailModel();

  std::string subscriberKey() const;

  int rowCount(QModelIndex const &parent = QModelIndex()) const override;
  int columnCount(QModelIndex const &parent = QModelIndex()) const override;
  QVariant data(QModelIndex const &index, int role) const override;
  QVariant headerData(
    int, Qt::Orientation, int role = Qt::DisplayRole) const override;
  bool isNumeric(int) const;
  bool isFactor(int) const;

  // NaN for unset
  double minEventTime() const { return minEventTime_; };
  double maxEventTime() const { return maxEventTime_; };

protected slots:
  void onChange(QList<ConfChange> const &);

signals:
  void receivedTuple(double time);
};

#endif
