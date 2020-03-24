#ifndef FACTORSEDITOR_H_200324
#define FACTORSEDITOR_H_200324
/* Small floating widget with a selectable list of factors */
#include <QWidget>

class QVBoxLayout;

class FactorsEditor : public QWidget
{
  Q_OBJECT

  QVBoxLayout *layout;

public:
  FactorsEditor(QStringList const &columns, QWidget *parent = nullptr);

  void setCurrentFactors(QStringList const &factors);

  QStringList currentFactors() const;
};

#endif
