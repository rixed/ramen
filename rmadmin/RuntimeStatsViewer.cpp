#include <QtGlobal>
#include <QDebug>
#include <QFormLayout>
#include <QLabel>
#include "confValue.h"
#include "RuntimeStatsViewer.h"

RuntimeStatsViewer::RuntimeStatsViewer(QWidget *parent) :
  AtomicWidget(parent)
{
  QFormLayout *layout = new QFormLayout;
# define ADD_LABEL(title, var) \
    do { \
      var = new QLabel; \
      layout->addRow(tr(title ":"), var); \
    } while (0);
# define ADD_LABEL_UNIT(title, unit, var) \
    ADD_LABEL(title " (" unit ")", var)
  ADD_LABEL("Stats Time", statsTime);
  ADD_LABEL("First Startup", firstStartup);
  ADD_LABEL("Last Startup", lastStartup);
  ADD_LABEL("Min. Event-Time", minEventTime);
  ADD_LABEL("Max. Event-Time", maxEventTime);
  ADD_LABEL("First Input", firstInput);
  ADD_LABEL("Last Input", lastInput);
  ADD_LABEL("First Output", firstOutput);
  ADD_LABEL("Last Output", lastOutput);
  // Input can be decomposed in Received and Selected:
  ADD_LABEL("Received Values", totInputTuples);
  ADD_LABEL("Selected Values", totSelectedTuples);
  ADD_LABEL("Filtered Values", totFilteredTuples);
  ADD_LABEL("Output Values", totOutputTuples);
  ADD_LABEL_UNIT("Avg. Full Output Size", "bytes", avgFullBytes);
  ADD_LABEL("Current Group Count", curGroups);
  ADD_LABEL("Maximum Group count", maxGroups);
  ADD_LABEL_UNIT("Received Volume", "bytes", totInputBytes);
  ADD_LABEL_UNIT("Output Volume", "bytes", totOutputBytes);
  ADD_LABEL_UNIT("Waited For Input", "secs", totWaitIn);
  ADD_LABEL_UNIT("Blocked During Output", "secs", totWaitOut);
  ADD_LABEL("Firing Notifications", totFiringNotifs);
  ADD_LABEL("Extinguished Notifications", totExtinguishedNotifs);
  ADD_LABEL_UNIT("CPU Spent", "secs", totCpu);
  ADD_LABEL_UNIT("Current RAM Allocated", "bytes", curRam);
  ADD_LABEL_UNIT("Max RAM Allocated", "bytes", maxRam);

  QWidget *w = new QWidget;
  w->setLayout(layout);
  relayoutWidget(w);
}

bool RuntimeStatsViewer::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::RuntimeStats const> s =
    std::dynamic_pointer_cast<conf::RuntimeStats const>(v);
  if (s) {
#   define SET_DATE(var) var->setText(stringOfDate(s->var))
    SET_DATE(statsTime);
    SET_DATE(firstStartup);
    SET_DATE(lastStartup);
#   define SET_OPT_DATE(var) \
      var->setText(s->var.has_value() ? stringOfDate(*s->var) : "n.a.")
    SET_OPT_DATE(minEventTime);
    SET_OPT_DATE(maxEventTime);
    SET_OPT_DATE(firstInput);
    SET_OPT_DATE(lastInput);
    SET_OPT_DATE(firstOutput);
    SET_OPT_DATE(lastOutput);
#   define SET_NUM(var) var->setText(QString::number(s->var))
    SET_NUM(totInputTuples);
    SET_NUM(totSelectedTuples);
    SET_NUM(totFilteredTuples);
    SET_NUM(totOutputTuples);
    if (s->totFullBytesSamples > 0) {
      double avg = (double)s->totFullBytes / s->totFullBytesSamples;
      avgFullBytes->setText(
        QString::number(avg) + QString(" (from ") +
        QString::number(s->totFullBytesSamples) + QString(" samples)"));
    } else {
      avgFullBytes->setText("no samples");
    }
    SET_NUM(curGroups);
    SET_NUM(maxGroups);
    SET_NUM(totInputBytes);
    SET_NUM(totOutputBytes);
    SET_NUM(totWaitIn);
    SET_NUM(totWaitOut);
    SET_NUM(totFiringNotifs);
    SET_NUM(totExtinguishedNotifs);
    SET_NUM(totCpu);
    SET_NUM(curRam);
    SET_NUM(maxRam);
    return true;
  } else {
    qCritical() << "Not a RuntimeStats value?!";
    return false;
  }
}
