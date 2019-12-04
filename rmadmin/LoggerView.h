#ifndef LOGGERVIEW_H_191202
#define LOGGERVIEW_H_191202

#include <QWidget>
#include <QListView>
#include <QStringListModel>

class LoggerView : public QListView
{
  Q_OBJECT
  QStringListModel *model;

public:
  LoggerView(QWidget *parent = nullptr);

public slots:
  void addLog(const QString &);
};

#endif
