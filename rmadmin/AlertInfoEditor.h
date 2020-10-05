#ifndef ALERTINFOEDITOR_H_191129
#define ALERTINFOEDITOR_H_191129
#include <memory>
#include <string>
#include <QTreeView>
#include <QWidget>
#include "AlertInfo.h"
#include "AtomicWidget.h"

class FilterEditor;
class QCheckBox;
class QCompleter;
class QLabel;
class QLineEdit;
class QRadioButton;

namespace conf {
  class Value;
};

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


class AlertInfoEditor : public AtomicWidget
{
  Q_OBJECT

  // Some error messages shown/hidden depending on selection:
  QLabel *inexistantSourceError;
  QLabel *mustSelectAField;

  /* In case the table/column is not in the source, also save the values
   * here. Use the accessors getTable/getColumn to get the actual version
   * either from the edition widget or from those saved values: */
  std::string _table, _column;

public:
  /* Just the fq and the field name, with no site (alert info selects from
   * all sites) */
  NameTreeView *source;

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
  QLineEdit *timeStep;
  QLineEdit *id;
  QLineEdit *descTitle;
  QLineEdit *descFiring;
  QLineEdit *descRecovery;
  QLineEdit *top;
  QLineEdit *carry;
  QCompleter *topCompleter = nullptr;
  QCompleter *carryCompleter = nullptr;
  QLabel *description;
  FilterEditor *where, *having;

  AlertInfoEditor(QWidget *parent = nullptr);
  void setEnabled(bool) override;
  std::shared_ptr<conf::Value const> getValue() const override;
  bool hasValidInput() const override;

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>) override;

protected slots:
  void checkSource(QModelIndex const &) const;
  void updateDescription();
  void updateFilters(QModelIndex const &);
};

#endif
