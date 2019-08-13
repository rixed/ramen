#ifndef PROCESSESWIDGETPROXY_H_190813
#define PROCESSESWIDGETPROXY_H_190813
#include <QSortFilterProxyModel>

/* This is the filtering-sorting datamodel proxy used (exclusively) by
 * ProcessesWidget.
 */

class QModelIndex;

class ProcessesWidgetProxy : public QSortFilterProxyModel
{
  Q_OBJECT

  bool includeTopHalves, includeStopped;

public:
  ProcessesWidgetProxy(QObject * = nullptr);

protected:
  bool filterAcceptsRow(int, QModelIndex const &) const override;

public slots:
  void viewTopHalves(bool checked);
  void viewStopped(bool checked);
};

#endif
