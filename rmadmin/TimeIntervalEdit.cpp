#include <cassert>
#include <iostream>
#include <QButtonGroup>
#include <QDateTimeEdit>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QRadioButton>
#include <QStackedLayout>
#include <QVBoxLayout>
#include "TimeIntervalEdit.h"

static bool const verbose = true;

TimeIntervalEdit::TimeIntervalEdit(QWidget *parent) :
  QPushButton(tr("Last XXX seconds (TODO)"), parent),
  currentSince(-600.), currentUntil(0.)
{
  selectLastSeconds = new QRadioButton(tr("View last…"));
  selectFixedInterval = new QRadioButton(tr("View range…"));
  QButtonGroup *radioGroup = new QButtonGroup();
  radioGroup->addButton(selectLastSeconds);
  radioGroup->addButton(selectFixedInterval);

  // TODO: parse time units
  lastSeconds = new QLineEdit;
  QDateTime const now(QDateTime::currentDateTime());
  beginInterval = new QDateTimeEdit(now.addSecs(-600));
  beginInterval->setCalendarPopup(true);
  endInterval = new QDateTimeEdit(now);
  endInterval->setCalendarPopup(true);
  buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Cancel | QDialogButtonBox::Apply);

  // Layout of the pop-up:
  QVBoxLayout *outerVBox = new QVBoxLayout;
  {
    QHBoxLayout *outerHBox = new QHBoxLayout;
    {
      QVBoxLayout *radioBox = new QVBoxLayout;
      radioBox->addWidget(selectLastSeconds);
      radioBox->addWidget(selectFixedInterval);
      outerHBox->addLayout(radioBox);
    }

    {
      stackedLayout = new QStackedLayout;
      {
        QVBoxLayout *relativeVLayout = new QVBoxLayout;
        {
          QHBoxLayout *relativeHLayout = new QHBoxLayout;
          relativeHLayout->addWidget(lastSeconds);
          relativeHLayout->addWidget(new QLabel(tr("seconds")));
          relativeVLayout->addLayout(relativeHLayout);
        }
        relativeVLayout->addStretch();
        QWidget *relativeWidget = new QWidget;
        relativeWidget->setLayout(relativeVLayout);
        stackedLayout->addWidget(relativeWidget);
      }

      {
        QFormLayout *absoluteLayout = new QFormLayout;
        absoluteLayout->addRow(tr("Since:"), beginInterval);
        absoluteLayout->addRow(tr("Until:"), endInterval);
        QWidget *absoluteWidget = new QWidget;
        absoluteWidget->setLayout(absoluteLayout);
        stackedLayout->addWidget(absoluteWidget);
      }
      outerHBox->addLayout(stackedLayout);
    }
    outerVBox->addLayout(outerHBox);
  }
  {
    QHBoxLayout *buttonBoxLayout = new QHBoxLayout;
    buttonBoxLayout->addStretch();
    buttonBoxLayout->addWidget(buttonBox);
    outerVBox->addLayout(buttonBoxLayout);
  }
  popup = new QWidget(parent);
  popup->setLayout(outerVBox);
  popup->setWindowFlags(Qt::Popup);
  popup->hide();

  /* Let the popup open when the user clicks on this button: */
  connect(this, &QPushButton::clicked,
          this, &TimeIntervalEdit::wantOpen);

  /* When user submits the form, then Recompute this button text, emit
   * valueChanged and close the popup.
   * Note: "Apply" button does not trigger the `accepted` signal so we
   * use the generic `clicked` and will check what button this is about. */
  connect(buttonBox, &QDialogButtonBox::clicked,
          this, &TimeIntervalEdit::wantSubmit);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &TimeIntervalEdit::wantCancel);

  /* When the user toggle the radio button to select which time selector
   * is meant to be used, then disable the other one: */
  connect(selectLastSeconds, &QRadioButton::toggled,
          this, &TimeIntervalEdit::updateEnabled);
}

void TimeIntervalEdit::updateEnabled()
{
  bool const relativeTimes = selectLastSeconds->isChecked();
  stackedLayout->setCurrentIndex(relativeTimes ? 0 : 1);
}

void TimeIntervalEdit::wantOpen()
{
  if (verbose)
    std::cout << "TimeIntervalEdit::wantOpen()" << std::endl;

  if (currentSince <= 0.) {
    /* Relative times: */
    assert(currentUntil < 1000000.);
    selectLastSeconds->setChecked(true);
    updateEnabled();
    lastSeconds->setText(QString::number(-currentSince));
  } else {
    /* Absolute times: */
    assert(currentUntil >= currentSince);
    selectFixedInterval->setChecked(true);
    updateEnabled();
    beginInterval->setDateTime(QDateTime::fromSecsSinceEpoch(currentSince));
    endInterval->setDateTime(QDateTime::fromSecsSinceEpoch(currentUntil));
  }

  popup->move(mapToGlobal(QPoint(0, height())));
  popup->show();
}

void TimeIntervalEdit::wantSubmit(QAbstractButton *button)
{
  if (button != buttonBox->button(QDialogButtonBox::Apply)) return;

  if (verbose)
    std::cout << "TimeIntervalEdit::wantSubmit()" << std::endl;

  bool const relativeTimes = selectLastSeconds->isChecked();

  // Save current values and reset button text:
  if (relativeTimes) {
    double const duration = lastSeconds->text().toDouble();
    currentSince = - duration;
    currentUntil = 0.;
    setText("Last " + QString::number(duration) + " seconds");
  } else {
    double const t1 = beginInterval->dateTime().toSecsSinceEpoch();
    double const t2 = endInterval->dateTime().toSecsSinceEpoch();
    currentSince = std::min(t1, t2);
    currentUntil = std::max(t1, t2);
    setText(QDateTime::fromSecsSinceEpoch(currentSince).toString() +
            " → " +
            QDateTime::fromSecsSinceEpoch(currentUntil).toString());
  }


  // Emits valueChanged:
  emit valueChanged(currentSince, currentUntil);

  // Close the popup:
  popup->hide();
}

void TimeIntervalEdit::wantCancel()
{
  if (verbose)
    std::cout << "TimeIntervalEdit::wantCancel()" << std::endl;

  popup->hide();
}
