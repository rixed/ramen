#ifndef ALERTINGWINDOW_H_200525
#define ALERTINGWINDOW_H_200525
#include "SavedWindow.h"

class AlertingJournal;
class AlertingStats;
class AlertingTimeLine;

class AlertingWin : public SavedWindow
{
  Q_OBJECT

  AlertingStats *stats;
  AlertingTimeLine *timeLine;
  AlertingJournal *journal;

public:
  AlertingWin(QWidget *parent = nullptr);
};

#endif
