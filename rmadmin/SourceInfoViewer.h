#ifndef SOURCEINFOVIEWER_H_190801
#define SOURCEINFOVIEWER_H_190801
#include <vector>
#include "AtomicWidget.h"

class QVBoxLayout;

class SourceInfoViewer : public AtomicWidget
{
  Q_OBJECT

  QVBoxLayout *layout;

public:
  SourceInfoViewer(conf::Key const &, QWidget *parent = nullptr);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
