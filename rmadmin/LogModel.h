#ifndef LOGMODEL_H_20191217
#define LOGMODEL_H_20191217
/*
 * Model storing log lines.
 *
 * Similar to QAbstractListModel but can store other data roles than display
 * and editor.
 */
#include <QAbstractListModel>
#include <QDateTime>
#include <QString>
#include <QVector>

class LogModel : public QAbstractListModel
{
  Q_OBJECT

  struct LogEntry {
    QDateTime time;
    QtMsgType type;
    QString msg;
  };

  QVector<LogEntry> logs;
  int first;
  bool removingFirst; // Set when in the middle of teh removal of the first line

public:
  LogModel(QObject *parent = nullptr);

  QVariant data(const QModelIndex &, int = Qt::DisplayRole) const override;

  QVariant headerData(
    int section, Qt::Orientation, int = Qt::DisplayRole) const override;

  int rowCount(const QModelIndex & = QModelIndex()) const override;

  int columnCount(const QModelIndex &index = QModelIndex()) const override
  {
    if (index.isValid()) return 0;
    return 3;
  }

public slots:
  void append(QtMsgType, QString const &);
};

#endif
