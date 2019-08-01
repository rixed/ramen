#ifndef CODEINFOPANEL_H_190605
#define CODEINFOPANEL_H_190605
#include <memory>
#include <QWidget>
#include "AtomicWidget.h"
#include "confValue.h"

class QLabel;
class QGridLayout;
class QGroupBox;
class RCEntryEditor;

class CodeInfoPanel : public AtomicWidget
{
  Q_OBJECT

  QWidget *widget;

  QGridLayout *infoLayout;
  QLabel *md5Label, *errLabel;
  QGroupBox *paramBox, *functionBox;
  RCEntryEditor *runBox;

  void setInfoVisible(bool visible);
public:
  CodeInfoPanel(QString const &sourceName, QWidget *parent = nullptr);
  void setEnabled(bool) {} // not editable

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
