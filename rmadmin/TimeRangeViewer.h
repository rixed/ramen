#ifndef TIMERANGEVIEWER_H_190801
#define TIMERANGEVIEWER_H_190801
#include "AtomicWidget.h"

/* Although time-ranges are read-only, all "editors" must be AtomicWidgets.
 */

class QTableWidget;

class TimeRangeViewer : public AtomicWidget
{
  Q_OBJECT

  QTableWidget *table;

  void extraConnections(KValue *);

public:
  TimeRangeViewer(QWidget *parent = nullptr);

public slots:
  bool setValue(KValue const *);

signals:
  void valueChanged(KValue const *);
};

#endif
