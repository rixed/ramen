#ifndef PROCESSESWIDGETPROXY_H_190813
#define PROCESSESWIDGETPROXY_H_190813
#include <QSortFilterProxyModel>

/* This is the filtering-sorting datamodel proxy used (exclusively) by
 * ProcessesWidget.
 */

class FunctionItem;
class QModelIndex;

class ProcessesWidgetProxy : public QSortFilterProxyModel
{
  Q_OBJECT

  bool includeFinished, includeUnused, includeDisabled, includeNonRunning,
       includeTopHalves;

public:
  ProcessesWidgetProxy(QObject * = nullptr);

protected:
  bool filterAcceptsRow(int, QModelIndex const &) const override;
  bool filterAcceptsFunction(FunctionItem const &) const;

public slots:
  void viewFinished(bool);
  void viewUnused(bool);
  void viewDisabled(bool);
  void viewNonRunning(bool);
  void viewTopHalves(bool);
};

#endif
