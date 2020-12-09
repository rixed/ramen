#ifndef ALERTINFOEDITOR_H_191129
#define ALERTINFOEDITOR_H_191129
#include <memory>
#include <optional>
#include <set>
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
class QListView;
class QPushButton;
class QRadioButton;
class QStringListModel;

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

  /* Similarly for groupBy fields: */
  std::optional<std::set<std::string>> _groupBy;

  /* Returns the current selection of group-by fields as a QStringList
   * (empty list for automatic group-by as well as explicit empty group-by,
   * used only to build the button label): */
  QStringList const getGroupByQStrings() const;

  /* Just the fq and the field name, with no site (alert info selects from
   * all sites) */
  NameTreeView *source;

  /* By default, group-by is implicit (automatic): */
  QCheckBox *autoGroupBy;
  /* Multi-Select fields of the selected table:
   * TODO: whenever the selected source is changed, and that result in a
   * table change, then reset this combo. Then, if the new table is _table
   * re-select the fields in _groupBy. */
  QListView *groupBy;
  /* So that groupBy's QSelectionModel does not change it is assigned a
   * single model that is repopulated every time the selected table changes: */
  QStringListModel *tableFields;
  /* The above QListView opens/closes when this button, which label list
   * the selected fields, is pushed: */
  QPushButton *openGroupBy;

  void updateGroupByLabel();

public:
  /* These functions will return the selected table and column (either from
   * the NameTreeView or the saved table and column values: */
  std::string const getTable() const;
  std::string const getColumn() const;
  std::optional<std::set<std::string>> const getGroupBy() const;

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
  QLineEdit *carryFields;
  /* TODO: carry_csts */
  QCompleter *topCompleter = nullptr;
  QCompleter *carryFieldsCompleter = nullptr;
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
  void checkGroupBy(QModelIndex const &);
  void updateDescription();
  void updateFilters(QModelIndex const &);
  void toggleGroupByView();
  void toggleAutoGroupBy(int);
};

#endif
