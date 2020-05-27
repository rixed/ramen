#ifndef ALERTINGJOURNAL_H_200615
#define ALERTINGJOURNAL_H_200615
#include <QWidget>

class AlertingLogsModel;
class QTableView;

class AlertingJournal : public QWidget {
  Q_OBJECT

  QTableView *tableView;

public:
  AlertingJournal(AlertingLogsModel *model, QWidget *parent = nullptr);

private slots:
  void resizeColumns();
};
#endif
