#ifndef WORKERVIEWER_H_190801
#define WORKERVIEWER_H_190801
#include <vector>
#include "AtomicWidget.h"

class QCheckBox;
class QLabel;
class QVBoxLayout;
class QFormLayout;

class WorkerViewer : public AtomicWidget
{
  Q_OBJECT

  QCheckBox *enabled, *debug, *used;
  QLabel *reportPeriod, *srcPath, *workerSign, *binSign;
  QLabel *role;
  QFormLayout *params;
  QVBoxLayout *parents;
  std::vector<QLabel *> parentLabels;

public:
  WorkerViewer(conf::Key const &, QWidget *parent = nullptr);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
