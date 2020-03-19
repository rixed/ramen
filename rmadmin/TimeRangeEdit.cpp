#include <cassert>
#include <QButtonGroup>
#include <QDateTimeEdit>
#include <QDebug>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QRadioButton>
#include <QStackedLayout>
#include <QVBoxLayout>
#include "TimeRangeEdit.h"

static bool const verbose(false);

TimeRangeEdit::TimeRangeEdit(QWidget *parent) :
  QPushButton(tr("Last XXX seconds (TODO)"), parent),
  range(true, -600., 0.)
{
  selectLastSeconds = new QRadioButton(tr("last…"));
  selectLastSeconds->setChecked(true);
  selectFixedRange = new QRadioButton(tr("range…"));
  QButtonGroup *radioGroup = new QButtonGroup();
  radioGroup->addButton(selectLastSeconds);
  radioGroup->addButton(selectFixedRange);

  // TODO: parse time units
  lastSeconds = new QLineEdit;
  QDateTime const now(QDateTime::currentDateTime());
  beginRange = new QDateTimeEdit(now.addSecs(-600));
  beginRange->setCalendarPopup(true);
  endRange = new QDateTimeEdit(now);
  endRange->setCalendarPopup(true);
  buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Cancel | QDialogButtonBox::Apply);

  // Layout of the pop-up:
  QVBoxLayout *outerVBox = new QVBoxLayout;
  {
    QHBoxLayout *outerHBox = new QHBoxLayout;
    {
      QVBoxLayout *radioBox = new QVBoxLayout;
      radioBox->addWidget(selectLastSeconds);
      radioBox->addWidget(selectFixedRange);
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
        absoluteLayout->addRow(tr("Since:"), beginRange);
        absoluteLayout->addRow(tr("Until:"), endRange);
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

  updateLabel();

  /* Let the popup open when the user clicks on this button: */
  connect(this, &QPushButton::clicked,
          this, &TimeRangeEdit::wantOpen);

  /* When user submits the form, then Recompute this button text, emit
   * valueChanged and close the popup.
   * Note: "Apply" button does not trigger the `accepted` signal so we
   * use the generic `clicked` and will check what button this is about. */
  connect(buttonBox, &QDialogButtonBox::clicked,
          this, &TimeRangeEdit::wantSubmit);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &TimeRangeEdit::wantCancel);

  /* When the user toggle the radio button to select which time selector
   * is meant to be used, then disable the other one: */
  connect(selectLastSeconds, &QRadioButton::toggled,
          this, &TimeRangeEdit::updateEnabled);
}

void TimeRangeEdit::updateEnabled()
{
  bool const relativeTimes = selectLastSeconds->isChecked();
  stackedLayout->setCurrentIndex(relativeTimes ? 0 : 1);
}

void TimeRangeEdit::offset(double dt)
{
  if (! range.relative) {
    range.since += dt;
    range.until += dt;
  }

  emit valueChanged(range);
}

void TimeRangeEdit::wantOpen()
{
  if (verbose)
    qDebug() << "TimeRangeEdit::wantOpen()";

  if (range.relative) {
    /* Relative times: */
    assert(range.since < 1000000.);
    selectLastSeconds->setChecked(true);
    updateEnabled();
    lastSeconds->setText(QString::number(-range.since));
  } else {
    /* Absolute times: */
    assert(range.until >= range.since);
    selectFixedRange->setChecked(true);
    updateEnabled();
    beginRange->setDateTime(QDateTime::fromSecsSinceEpoch(range.since));
    endRange->setDateTime(QDateTime::fromSecsSinceEpoch(range.until));
  }

  popup->move(mapToGlobal(QPoint(0, height())));
  popup->show();
}

void TimeRangeEdit::updateLabel()
{
  setText(range.toQString());
}

void TimeRangeEdit::wantSubmit(QAbstractButton *button)
{
  if (button != buttonBox->button(QDialogButtonBox::Apply)) return;

  if (verbose)
    qDebug() << "TimeRangeEdit::wantSubmit()";

  range.relative = selectLastSeconds->isChecked();

  // Save current values and reset button text:
  if (range.relative) {
    range.since = - lastSeconds->text().toDouble();
    range.until = 0.;
  } else {
    double const t1 = beginRange->dateTime().toSecsSinceEpoch();
    double const t2 = endRange->dateTime().toSecsSinceEpoch();
    range.since = std::min(t1, t2);
    range.until = std::max(t1, t2);
  }

  // Update button label
  updateLabel();

  // Emits valueChanged:
  emit valueChanged(range);

  // Close the popup:
  popup->hide();
}

void TimeRangeEdit::wantCancel()
{
  if (verbose)
    qDebug() << "TimeRangeEdit::wantCancel()";

  popup->hide();
}
