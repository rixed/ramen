#ifndef LOGGERVIEW_H_191202
#define LOGGERVIEW_H_191202
/* Widget to display/search the last LOG_HISTORY log messages */
#include <QFile>
#include <QSize>
#include <QStyledItemDelegate>
#include <QTextStream>
#include <QWidget>

class QModelIndex;
class QPainter;

/*
 * Draws log items:
 */

class LogLine : public QStyledItemDelegate
{
  Q_OBJECT

public:
  LogLine(QObject *parent = nullptr) : QStyledItemDelegate(parent) {}

  void paint(
    QPainter *, QStyleOptionViewItem const &, QModelIndex const &) const override;

  QSize sizeHint(
    QStyleOptionViewItem const &, QModelIndex const &) const override;

};

/*
 * Widget displaying all logs:
 */

class QCheckBox;
class QPushButton;
class QTableView;

class LoggerView : public QWidget
{
  Q_OBJECT

  QTableView *tableView;
  unsigned resizeSkip;
  bool doScroll;
  bool saveFileClosed;
  QFile saveFile;
  QCheckBox *saveLogs;
  QPushButton *saveButton;

public:
  LoggerView(QWidget *parent = nullptr);
  ~LoggerView();
  void flush();
  void setModel(QAbstractItemModel *);
protected:
  void saveLines(QModelIndex const &, int, int);

protected slots:
  void resizeColumns();
  void setDoScroll(bool);
  void pickSaveFile();
  void onNewRows(QModelIndex const &, int, int);
};

#endif
