#ifndef PROCESSESWIDGET_H_190806
#define PROCESSESWIDGET_H_190806
#include <QWidget>

/* A tree to display the sites/programs/workers. */

class GraphModel;
class QTreeView;
class QLineEdit;
class MyProxy;
class ProgramItem;
class FunctionItem;

class ProcessesWidget : public QWidget
{
  Q_OBJECT

public:
  QTreeView *treeView;
  QLineEdit *searchBox;
  QWidget *searchFrame;
  MyProxy *proxyModel;

  ProcessesWidget(GraphModel *, QWidget *parent = nullptr);

  QSize sizeHint() const { return QSize(800, 494); }

protected slots:
  void adjustColumnSize(
    QModelIndex const &, QModelIndex const &, QVector<int> const &);
  void openSearch();
  void changeSearch(QString const &);
  void closeSearch();
  void wantEdit(ProgramItem const *);
  void wantTable(FunctionItem *);
  void activate(QModelIndex const &);
};

#endif
