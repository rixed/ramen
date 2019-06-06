#ifndef CODEINFOPANEL_H_190605
#define CODEINFOPANEL_H_190605
#include <memory>
#include <QWidget>
#include "AtomicWidget.h"
#include "confValue.h"

class QLabel;
class QGridLayout;
class QGroupBox;

class CodeInfoPanel : public QWidget, public AtomicWidget
{
  Q_OBJECT

  QGridLayout *infoLayout;
  QLabel *md5Label, *condRunLabel, *errLabel;
  QGroupBox *paramBox, *functionBox;

  void setInfoVisible(bool visible);
public:
  CodeInfoPanel(QString const &sourceName, QWidget *parent = nullptr);

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
