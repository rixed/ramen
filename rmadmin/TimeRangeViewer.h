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

public:
  TimeRangeViewer(QWidget *parent = nullptr);
  void setEnabled(bool) {}

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
