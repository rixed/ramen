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
  QLabel *totInputTuples, *totSelectedTuples, *totOutputTuples;
  QLabel *avgFullBytes;
  QLabel *curGroups;
  QLabel *totInputBytes, *totOutputBytes;
  QLabel *totWaitIn, *totWaitOut;
  QLabel *totFiringNotifs, *totExtinguishedNotifs;
  QLabel *totCpu;
  QLabel *curRam, *maxRam;

  void extraConnections(KValue *);

public:
  RuntimeStatsViewer(QWidget *parent = nullptr);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
