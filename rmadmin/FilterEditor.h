#ifndef FILTEREDITOR_H_191210
#define FILTEREDITOR_H_191210
#include <QString>
#include <QWidget>
#include "AlertInfo.h"

class QComboBox;
class QLineEdit;
class QModelIndex;
class NamesSubtree;
class SimpleFilter;

class FilterEditor : public QWidget
{
  Q_OBJECT

public:
  QLineEdit *lhsEdit;
  QComboBox *opEdit;
  QLineEdit *rhsEdit;

  FilterEditor(QWidget *parent = nullptr);
  void setEnabled(bool);

  QString const description(
    QString const &prefix = QString(), QString const &suffix = QString());

  bool setValue(SimpleFilter const &);
  void clear();

public slots:
  void setFunction(QModelIndex const &);

signals:
  /* Signaled when the filter has been changed in any way: */
  void inputChanged() const;
};

#endif
