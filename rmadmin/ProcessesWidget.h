#ifndef PROCESSESWIDGET_H_190806
#define PROCESSESWIDGET_H_190806
#include <memory>
#include <QWidget>

/* A tree to display the sites/programs/workers. */

class GraphModel;
class QTreeView;
class QLineEdit;
class ProcessesWidgetProxy;
class ProgramItem;
struct Program;
class Function;

class ProcessesWidget : public QWidget
{
  Q_OBJECT

public:
  QTreeView *treeView;
  QLineEdit *searchBox;
  QWidget *searchFrame;
  ProcessesWidgetProxy *proxyModel;

  ProcessesWidget(GraphModel *, QWidget *parent = nullptr);

  QSize sizeHint() const { return QSize(800, 494); }

public slots:
  void adjustColumnSize(
    QModelIndex const &, QModelIndex const &, QVector<int> const &);
  void adjustAllColumnSize();
  void openSearch();
  void changeSearch(QString const &);
  void closeSearch();
  void wantEdit(std::shared_ptr<Program const>);
  void wantTable(std::shared_ptr<Function>);
  void activate(QModelIndex const &);
  void expandRows(QModelIndex const &parent, int first, int last);
};

#endif
