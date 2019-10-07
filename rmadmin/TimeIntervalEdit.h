#ifndef TIMEINTERVALEDIT_H_191007
#define TIMEINTERVALEDIT_H_191007
#include <QPushButton>

class QDateTimeEdit;
class QDialogButtonBox;
class QLineEdit;
class QRadioButton;
class QStackedLayout;

class TimeIntervalEdit : public QPushButton
{
  Q_OBJECT

  /* Time control: we can select to see the last X seconds, and the actual
   * time range will be computed at every refresh, or we can select a begin
   * and end time. */
  QRadioButton *selectLastSeconds;
  QRadioButton *selectFixedInterval;
  QLineEdit *lastSeconds;
  QDateTimeEdit *beginInterval, *endInterval;
  QStackedLayout *stackedLayout;
  QWidget *popup;
  QDialogButtonBox *buttonBox;

  double currentSince, currentUntil;

public:
  TimeIntervalEdit(QWidget *parent = nullptr);

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
