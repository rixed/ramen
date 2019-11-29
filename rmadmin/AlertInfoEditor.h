#ifndef ALERTINFOEDITOR_H_191129
#define ALERTINFOEDITOR_H_191129
#include <memory>
#include <string>
#include <QTreeView>
#include <QWidget>
#include "AlertInfo.h"

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
  void currentChanged(QModelIndex const &, QModelIndex const &) override;

signals:
  void selectedChanged(QModelIndex const &);
};


class AlertInfoV1Editor : public QWidget
{
  Q_OBJECT

  // Some error messages shown/hidden depending on selection:
  QLabel *inexistantSourceError;
  QLabel *mustSelectAField;

public:
  /* Just the fq and the field name, with no site (alert info v1 selects from
   * all sites) */
  NameTreeView *source;
  /* In case the table/column is not in the source, also save the values
   * here: */
  std::string table, column;

  /* These functions will return the selected table and column (either from
   * the NameTreeView or the saved table and column values: */
  std::string const getTable() const;
  std::string const getColumn() const;

  QCheckBox *isEnabled; // the editor, not the alert
  QRadioButton *thresholdIsMax;
  QRadioButton *thresholdIsMin;
  QLineEdit *threshold;
  QLineEdit *hysteresis;
  QLineEdit *duration;
  QLineEdit *percentage;
  double timeStep;  // TODO?
  QLineEdit *id;
  QLineEdit *descTitle;
  QLineEdit *descFiring;
  QLineEdit *descRecovery;
  QLabel *description;

  AlertInfoV1Editor(QWidget *parent = nullptr);
  void setEnabled(bool);
  bool setValue(AlertInfoV1 const &);
  std::unique_ptr<AlertInfoV1> getValue() const;

protected slots:
  void checkSource(QModelIndex const &current) const;
  void updateDescription() const;

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
