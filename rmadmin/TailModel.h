#ifndef TAILMODEL_H_190515
#define TAILMODEL_H_190515
#include <memory>
#include <QAbstractItemModel>
#include <QString>
#include <QStringList>
#include "KVPair.h"

/* The model representing lines of tuples, with possibly some tuples skipped
 * in between 2 lines. The model stores *all* tuples and is owned by a
 * function, that share it with 0 or several widgets. When the function is
 * the only user than it can, after a while, destroy it to reclaim memory.
 * The function will also delete its counted reference to the TailModel
 * whenever the worker change.
 *
 * All of this happen behind TailModel's back though, as the TailModel itself
 * is only given the identifier (site/fq/instance) it must subscribe to (and
 * unsubscribe at destruction), and an unserializing function (or rather, the
 * tuple RamenType).
 *
 * It then receive and store the tuples, as unserialized RamenValues.
 */

struct EventTime;
struct RamenValue;
struct RamenType;
namespace conf {
  class Value;
};

class TailModel : public QAbstractTableModel
{
  Q_OBJECT

  std::shared_ptr<EventTime const> eventTime;

public:
  QString const fqName;
  QString const workerSign;
  std::string const keyPrefix;

  std::vector<std::pair<double, std::unique_ptr<RamenValue const>>> tuples;
  std::shared_ptr<RamenType const> type;
  QStringList factors; // supposed to be a list of strings

  TailModel(
    QString const &fqName, QString const &workerSign,
    std::shared_ptr<RamenType const> type,
    QStringList factors,
    std::shared_ptr<EventTime const>,
    QObject *parent = nullptr);

  ~TailModel();

  std::string subscriberKey() const;

  int rowCount(QModelIndex const &parent = QModelIndex()) const override;
  int columnCount(QModelIndex const &parent = QModelIndex()) const override;
  QVariant data(QModelIndex const &index, int role) const override;
  QVariant headerData(int, Qt::Orientation, int role = Qt::DisplayRole) const override;
  bool isNumeric(int) const;
  bool isFactor(int) const;

protected slots:
  void addTuple(KVPair const &);
};

#endif
