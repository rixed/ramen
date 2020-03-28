#include <iostream>
#include <QCheckBox>
#include <QDebug>
#include <QFile>
#include <QFileDialog>
#include <QFont>
#include <QFontMetrics>
#include <QHeaderView>
#include <QPainter>
#include <QPushButton>
#include <QTableView>
#include <QVBoxLayout>
#include "LogModel.h"
#include "LoggerView.h"

static QVariant const color(QtMsgType type)
{
  switch (type) {
    case QtWarningMsg: return QColor(Qt::darkYellow);
    case QtCriticalMsg: return QColor(Qt::red);
    default: return QVariant();
  }
}

void LogLine::paint(
  QPainter *painter,
  QStyleOptionViewItem const &option,
  const QModelIndex &index) const
{
  painter->save();
  QFont font(painter->font());
  // Seems useless:
  font.setStyleHint(QFont::Monospace, QFont::PreferDevice);
  // This seems to do the right thing:
  font.setFamily("Courier New");
  painter->setFont(font);

  QString const text(index.data(Qt::DisplayRole).toString());

  QVariant const c(color(static_cast<QtMsgType>(index.data(Qt::UserRole).toInt())));
  if (c.canConvert<QColor>())
    painter->setPen(c.value<QColor>());

  painter->drawText(option.rect, Qt::AlignLeft | Qt::TextWordWrap, text);
  painter->restore();
}

QSize LogLine::sizeHint(
  QStyleOptionViewItem const &option, const QModelIndex &index) const
{
  QFont font(option.font);
  font.setStyleHint(QFont::Monospace, QFont::PreferDevice);
  font.setFamily("Courier New");
  QFontMetrics fontMetric(font);
  QString const text(index.data(Qt::DisplayRole).toString());
  return fontMetric.size(Qt::TextSingleLine, text);
}

LoggerView::LoggerView(QWidget *parent)
  : QWidget(parent),
    resizeSkip(0),
    doScroll(true),
    saveFileClosed(true),
    saveFile("/tmp/RmAdmin.log")
{
  tableView = new QTableView;

  tableView->setSelectionMode(QAbstractItemView::NoSelection);
  tableView->setShowGrid(false);
  LogLine *logLine = new LogLine(this);
  tableView->setItemDelegate(logLine);
  tableView->horizontalHeader()->setStretchLastSection(true);
  tableView->horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);
  tableView->verticalHeader()->hide();
  tableView->setEditTriggers(QAbstractItemView::NoEditTriggers);

  QCheckBox *scrollCheckBox = new QCheckBox(tr("Auto-scroll"));
  scrollCheckBox->setChecked(doScroll);

  saveLogs = new QCheckBox(tr("Also save in:"));
  saveLogs->setChecked(true);
  saveButton = new QPushButton(saveFile.fileName());

  QHBoxLayout *ctrlLine = new QHBoxLayout;
  ctrlLine->addWidget(saveLogs);
  ctrlLine->addWidget(saveButton);
  ctrlLine->addStretch();
  ctrlLine->addWidget(scrollCheckBox);

  QVBoxLayout *layout = new QVBoxLayout;
  setLayout(layout);
  layout->addLayout(ctrlLine);
  layout->addWidget(tableView);

  connect(scrollCheckBox, &QCheckBox::stateChanged,
          this, &LoggerView::setDoScroll);
  connect(saveButton, &QPushButton::clicked,
          this, &LoggerView::pickSaveFile);
}

LoggerView::~LoggerView()
{
  flush();
}

void LoggerView::flush()
{
  if (! saveFileClosed) saveFile.close();
}

void LoggerView::setModel(QAbstractItemModel *model)
{
  tableView->setModel(model);

  connect(model, &LogModel::rowsInserted,
          this, &LoggerView::resizeColumns);
  connect(model, &LogModel::rowsInserted,
          this, &LoggerView::onNewRows);
}

void LoggerView::resizeColumns()
{
  if (resizeSkip++ % 256 != 0) return;  // spare some CPU

  tableView->resizeColumnToContents(0);
  tableView->resizeColumnToContents(1);
}

void LoggerView::setDoScroll(bool s)
{
  doScroll = s;
}

void LoggerView::onNewRows(QModelIndex const &parent, int first, int last)
{
  if (doScroll)
    tableView->scrollToBottom();

  if (saveLogs->isChecked()) {
    if (saveFileClosed) {
      if (! saveFile.open(QIODevice::WriteOnly | QIODevice::Append |
                          QIODevice::Truncate | QIODevice::Text))
        qFatal("Cannot open log file %s",
               saveFile.fileName().toStdString().c_str());
      saveFileClosed = false;
      saveLines(parent, 0, last);
    }
    saveLines(parent, first, last);
  }
}

void LoggerView::saveLines(QModelIndex const &parent, int first, int last)
{
  for (int i = first; i <= last; i++) {
    QDateTime const time(tableView->model()->index(i, 0, parent).data().toDateTime());
    QString const msg(tableView->model()->index(i, 2, parent).data().toString());
    QString line(time.toString() + ": " + msg + "\n");
    saveFile.write(line.toLatin1());
  }
}

void LoggerView::pickSaveFile()
{
  QString fileName =
    QFileDialog::getSaveFileName(
      this, tr("Select a file where to append logs into"),
      "/tmp", tr("Log file (*)"), nullptr, QFileDialog::DontConfirmOverwrite);

  if (fileName.isNull()) {
    if (! saveFileClosed) {
      saveLogs->setChecked(false);
      saveFile.close();
      saveFileClosed = true;
    }
  } else {
    if (fileName != saveFile.fileName()) {
      if (! saveFileClosed) {
        saveFile.close();
        saveFileClosed = true;
      }
    }
  }

  saveFile.setFileName(fileName);
  saveButton->setText(fileName.isEmpty() ? tr("Choose") : fileName);
}
