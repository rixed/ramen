#ifndef ALERTINFO_H_190816
#define ALERTINFO_H_190816
#include <string>
#include <list>
#include <QWidget>
#include <QTreeView>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
}

class QLineEdit;
class QItemSelection;

struct AlertInfo {
  virtual ~AlertInfo() {}
  virtual QString const toQString() const = 0;
  virtual value toOCamlValue() const = 0;
  virtual bool operator==(AlertInfo const &o) const = 0;
};

class AlertInfoV1Editor;

struct AlertInfoV1 : public AlertInfo
{
  struct SimpleFilter {
    std::string lhs;
    std::string rhs;
    std::string op;

    SimpleFilter(value);

    value toOCamlValue() const;

    QWidget *editorWidget() const;
  };

  std::string table;
  std::string column;
  bool isEnabled;
  std::list<SimpleFilter> where;
  std::list<SimpleFilter> having;
  double threshold;
  double recovery;
  double duration;
  double ratio;
  /* TODO: Get rid of this, as it's too error-prone to rely on reaggregation
   * under any shape or form. */
  double timeStep;
  std::string id;
  std::string descTitle;
  std::string descFiring;
  std::string descRecovery;

  // Create an alert from an OCaml value:
  AlertInfoV1(value);

  // Create an alert from the editor values:
  AlertInfoV1(AlertInfoV1Editor const *);

  value toOCamlValue() const;

  QWidget *editorWidget() const;

  QString const toQString() const;

  bool operator==(AlertInfoV1 const &) const;
  bool operator==(AlertInfo const &) const;
};

class SimpleFilterEditor : public QWidget
{
  Q_OBJECT
public:
  QLineEdit *lhsEdit, *rhsEdit, *opEdit;
  SimpleFilterEditor(AlertInfoV1::SimpleFilter const *, QWidget *parent = nullptr);
  void setEnabled(bool);
};

class QCheckBox;
class QLineEdit;
class QRadioButton;
class QLabel;

/* Same as QTreeView but emits a selectedChanged whenever the current
 * entry changes.
 * Note: Has to be defined in a .h for the moc processor to find it. */
class NameTreeView : public QTreeView
{
  Q_OBJECT

public:
  NameTreeView(QWidget *parent = nullptr);

protected slots:
  void currentChanged(const QModelIndex &, const QModelIndex &) override;

signals:
  void selectedChanged(const QModelIndex &);
};


class AlertInfoV1Editor : public QWidget
{
  Q_OBJECT

  // Some error messages shown/hidden depending on selection:
  QLabel *inexistantSourceError;
  QLabel *mustSelectAField;

public:
  NameTreeView *source;
  /* In case the table/column is not in the source, also save the values
   * here: */
  std::string table, column;

  QCheckBox *isEnabled; // the editor, not the alert
  QRadioButton *thresholdIsMax;
  QRadioButton *thresholdIsMin;
  QLineEdit *threshold;
  QLineEdit *hysteresis;
  QLineEdit *duration;
  QLineEdit *ratio;
  double timeStep;  // TODO?
  QLineEdit *id;
  QLineEdit *descTitle;
  QLineEdit *descFiring;
  QLineEdit *descRecovery;

  AlertInfoV1Editor(QWidget *parent = nullptr);
  void setEnabled(bool);
  bool setValue(AlertInfoV1 const &);
  std::unique_ptr<AlertInfoV1> getValue() const;

protected slots:
  void checkSource(QModelIndex const &current) const;

signals:
  void inputChanged() const;
};

#include "AtomicWidget.h"
namespace conf {
  class Value;
};

class AlertInfoEditor : public AtomicWidget
{
  Q_OBJECT

  AlertInfoV1Editor *v1;

public:
  AlertInfoEditor(QWidget *parent = nullptr);

  std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
