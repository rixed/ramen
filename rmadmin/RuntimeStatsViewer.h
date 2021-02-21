#ifndef RUNTIMESTATSVIEWER_H_190801
#define RUNTIMESTATSVIEWER_H_190801
#include "AtomicWidget.h"

class QLabel;

class RuntimeStatsViewer : public AtomicWidget
{
  Q_OBJECT

  QLabel *statsTime, *firstStartup, *lastStartup;
  QLabel *minEventTime, *maxEventTime;
  QLabel *firstInput, *lastInput;
  QLabel *firstOutput, *lastOutput;
  QLabel *totInputTuples, *totSelectedTuples, *totFilteredTuples, *totOutputTuples;
  QLabel *avgFullBytes;
  QLabel *curGroups, *maxGroups;
  QLabel *totInputBytes, *totOutputBytes;
  QLabel *totWaitIn, *totWaitOut;
  QLabel *totFiringNotifs, *totExtinguishedNotifs;
  QLabel *totCpu;
  QLabel *curRam, *maxRam;

public:
  RuntimeStatsViewer(QWidget *parent = nullptr);
  void setEnabled(bool) {}

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
