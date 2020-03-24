#include <QtGlobal>
#include <QDebug>
#include <QFormLayout>
#include <QLabel>
#include <QCheckBox>
#include "misc.h"
#include "confWorkerRole.h"
#include "confRCEntryParam.h"
#include "confValue.h"
#include "WorkerViewer.h"

WorkerViewer::WorkerViewer(QWidget *parent) :
  AtomicWidget(parent)
{
  enabled = new QCheckBox("Enabled");
  enabled->setEnabled(false);
  debug = new QCheckBox("Debug");
  debug->setEnabled(false);
  used = new QCheckBox("In Use");
  used->setEnabled(false);
  QVBoxLayout *flagsLayout = new QVBoxLayout;
  flagsLayout->addWidget(enabled);
  flagsLayout->addWidget(debug);
  flagsLayout->addWidget(used);

  reportPeriod = new QLabel;
  cwd = new QLabel;
  workerSign = new QLabel;
  binSign = new QLabel;
  role = new QLabel;

  params = new QFormLayout;
  parents = new QVBoxLayout;

  QFormLayout *layout = new QFormLayout;
  layout->addRow(tr("Role:"), role);
  layout->addRow(new QLabel(tr("Parameters:")));
  layout->addRow(params);
  layout->addRow(new QLabel(tr("Parents:")));
  layout->addRow(parents);
  layout->addRow(tr("Flags:"), flagsLayout);
  layout->addRow(tr("Report Every:"), reportPeriod);
  layout->addRow(tr("Working Directory:"), cwd);
  layout->addRow(tr("Worker Signatures:"), workerSign);
  layout->addRow(tr("Binary Signatures:"), binSign);

  QWidget *w = new QWidget;
  w->setLayout(layout);
  relayoutWidget(w);
}

void WorkerViewer::setEnabled(bool enabled_)
{
  enabled->setEnabled(enabled_);
  debug->setEnabled(enabled_);
  used->setEnabled(enabled_);
}

bool WorkerViewer::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v)
{
  /* Empty the previous params/parents layouts: */
  while (params->count() > 0) params->removeRow(0);
  emptyLayout(parents);

  std::shared_ptr<conf::Worker const> w =
    std::dynamic_pointer_cast<conf::Worker const>(v);
  if (w) {
    enabled->setChecked(w->enabled);
    debug->setChecked(w->debug);
    used->setChecked(w->used);
    reportPeriod->setText(stringOfDuration(w->reportPeriod));
    cwd->setText(w->cwd);
    workerSign->setText(w->workerSign);
    binSign->setText(w->binSign);
    role->setText(w->role->toQString());
    if (w->params.size() == 0) {
      QLabel *none = new QLabel("<i>" + tr("none") + "</i>");
      none->setAlignment(Qt::AlignCenter);
      params->addRow(none);
    } else {
      for (auto p : w->params) {
        params->addRow(
          QString::fromStdString(p->name) + QString(":"),
          new QLabel (p->val ? p->val->toQString(std::string()) : QString()));
      }
    }
    if (w->parent_refs.size() == 0) {
      QLabel *none = new QLabel("<i>" + tr("none") + "</i>");
      none->setAlignment(Qt::AlignCenter);
      parents->addWidget(none);
    } else {
      for (auto const &p : w->parent_refs) {
        QLabel *l = new QLabel(p->toQString());
        l->setAlignment(Qt::AlignCenter);
        parents->addWidget(l);
      }
    }
    return true;
  } else {
    qCritical() << "Not a Worker value?!";
    return false;
  }
}
