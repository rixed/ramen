#include <QCheckBox>
#include <QDebug>
#include <QVBoxLayout>

#include "chart/FactorsEditor.h"

FactorsEditor::FactorsEditor(QStringList const &columnNames, QWidget *parent)
  : QWidget(parent)
{
  layout = new QVBoxLayout;

  for (int i = 0; i < columnNames.count(); i++) {
    QCheckBox *c = new QCheckBox(columnNames[i]);
    layout->addWidget(c);
  }

  setLayout(layout);
}

void FactorsEditor::setCurrentFactors(QStringList const &factors)
{
  for (int i = 0; i < layout->count(); i++) {
    QCheckBox *c(dynamic_cast<QCheckBox *>(layout->itemAt(i)->widget()));
    if (!c) {
      qWarning() << "FactorsEditor: not a QCheckBox in the layout:"
                 << layout->itemAt(i)->widget();
      continue;
    }
    c->setChecked(factors.contains(c->text()));
  }
}

QStringList FactorsEditor::currentFactors() const
{
  QStringList res;

  for (int i = 0; i < layout->count(); i++) {
    QCheckBox *c(dynamic_cast<QCheckBox *>(layout->itemAt(i)->widget()));
    if (!c) continue;
    if (c->isChecked()) res += c->text();
  }

  return res;
}
