#ifndef TIMERANGEEDIT_H_191007
#define TIMERANGEEDIT_H_191007
#include <QPushButton>
#include "TimeRange.h"

class QDateTimeEdit;
class QDialogButtonBox;
class QLineEdit;
class QRadioButton;
class QStackedLayout;

class TimeRangeEdit : public QPushButton
{
  Q_OBJECT

  /* Time control: we can select to see the last X seconds, and the actual
   * time range will be computed at every refresh, or we can select a begin
   * and end time. */
  QRadioButton *selectLastSeconds;
  QRadioButton *selectFixedRange;
  QLineEdit *lastSeconds;
  QDateTimeEdit *beginRange, *endRange;
  QStackedLayout *stackedLayout;
  QWidget *popup;
  QDialogButtonBox *buttonBox;

  void updateLabel();

public:
  TimeRange range;

  TimeRangeEdit(QWidget *parent = nullptr);

public slots:
  /* Move the time range by the specified amount, and emit valueChanged. */
  void offset(double dt);

protected slots:
  void wantOpen();
  void wantSubmit(QAbstractButton *);
  void wantCancel();
  void updateEnabled();

signals:
  void valueChanged(TimeRange const &);
};

#endif
