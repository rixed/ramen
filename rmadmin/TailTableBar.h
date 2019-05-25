#ifndef TAILTABLEBAR_H_190525
#define TAILTABLEBAR_H_190525
#include <QWidget>

class QPushButton;
class QComboBox;

class TailTableBar : public QWidget
{
  Q_OBJECT

  QPushButton *quickChartButton;
  QComboBox *addToCombo;

public:
  TailTableBar(QWidget *parent = nullptr);
  void setEnabled(bool);
};

#endif
