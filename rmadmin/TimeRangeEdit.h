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

  double currentSince, currentUntil;

  void updateLabel();

public:
  TimeRangeEdit(QWidget *parent = nullptr);

  // Return a relative or absolute TimeRange:
  TimeRange getRange() const;

protected slots:
  void wantOpen();
  void wantSubmit(QAbstractButton *);
  void wantCancel();
  void updateEnabled();

signals:
  /* Either send begin and end, or minus the duration in seconds and 0.
   * In other words, times <= 0 are relative to now while times > 0 are
   * absolute timestamps. */
  void valueChanged(double, double);
};

#endif
