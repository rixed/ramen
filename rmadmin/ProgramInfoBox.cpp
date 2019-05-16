#include <QLabel>
#include <QFormLayout>
#include "ProgramItem.h"
#include "KLineEdit.h"
#include "KLabel.h"
#include "KBool.h"
#include "ProgramInfoBox.h"

ProgramInfoBox::ProgramInfoBox(ProgramItem const *p_, QWidget *parent) :
  AtomicForm(p_->name, parent), p(p_)
{
  QWidget *cw = new QWidget(this);
  QFormLayout *layout = new QFormLayout;
  cw->setLayout(layout);
  setCentralWidget(cw);

  std::string pref("programs/" + p->name.toStdString() + "/");

  KBool *mustRun =
    new KBool(pref + "must_run", tr("run that program"), tr("do not run"));
  layout->addRow(tr("&Enabled"), mustRun);
  addWidget(mustRun);

  KBool *debug =
    new KBool(pref + "debug", tr("normal"), tr("verbose logs"));
  layout->addRow(tr("&Debug"), debug);
  addWidget(debug);

  KLineEdit *reportPeriod =
    new KLineEdit(pref + "report_period", conf::ValueType::FloatType, cw);
  layout->addRow(tr("&Reporting Interval"), reportPeriod);
  addWidget(reportPeriod);

  KLabel *binPath = new KLabel(pref + "bin_path", cw);
  layout->addRow(tr("Executable"), binPath);

  KLabel *srcPath = new KLabel(pref + "src_path", cw);
  layout->addRow(tr("Source File"), srcPath);

  // etc...
}
