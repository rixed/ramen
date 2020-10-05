#ifndef CONFTREEMODEL_H_200320
#define CONFTREEMODEL_H_200320
/* Abstract class to help build global treemodels from the configuration */
#include <QAbstractItemModel>

class ConfSubTree;
struct KValue;
class QStringList;

class ConfTreeModel : public QAbstractItemModel
{
  Q_OBJECT

public:
  ConfSubTree *root;

  ConfSubTree *findOrCreate(
    ConfSubTree *,
    // This QStringList will be emptied:
    QStringList &,
    /* Therefore this "const" QString must not belong to the above list.
     * To ensure this, it is purposefully passed as copy: */
    QString const termValue);

  ConfTreeModel(QObject *parent = nullptr);
  virtual ~ConfTreeModel();

  void dump() const;

  // The QAbstractModel:
  QModelIndex index(int, int, QModelIndex const &) const;
  QModelIndex parent(QModelIndex const &) const;
  int rowCount(QModelIndex const &) const;
  int columnCount(QModelIndex const &) const;
  QVariant data(QModelIndex const &, int = Qt::DisplayRole) const;

  QModelIndex find(std::string const &) const;

  bool isTerm(QModelIndex const &) const;
};

#endif
