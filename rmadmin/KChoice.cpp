#include <cassert>
#include <QDebug>
#include <QRadioButton>
#include <QVBoxLayout>
#include "confValue.h"
#include "KChoice.h"

KChoice::KChoice(
  std::vector<std::pair<QString const,
  std::shared_ptr<conf::Value const>>> labels,
  QWidget *parent)
  : AtomicWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  for (auto const &label : labels) {
    QRadioButton *b = new QRadioButton(label.first);
    choices.push_back({b, label.second});
    layout->addWidget(b);

    connect(b, &QRadioButton::released,
            this, &KChoice::inputChanged);
  }

  QWidget *w = new QWidget;
  w->setLayout(layout);
  relayoutWidget(w);
}

std::shared_ptr<conf::Value const> KChoice::getValue() const
{
  for (size_t i = 0; i < choices.size(); i++) {
    if (choices[i].first->isChecked())
      return choices[i].second;
  }

  qDebug() << "No radio button checked!?";
  return choices[0].second;
}

bool KChoice::setValue(
  std::string const &k, std::shared_ptr<conf::Value const> v)
{
  for (unsigned i = 0; i < choices.size(); i ++) {
    if (*choices[i].second == *v) {
      if (! choices[i].first->isChecked()) {
        choices[i].first->setChecked(true);
        emit valueChanged(k, v);
      }
      return true;
    }
  }

  qDebug() << "No choice correspond to value" << *v;
  return false;
}

void KChoice::setEnabled(bool enabled)
{
  for (auto &c : choices)
    c.first->setEnabled(enabled);
}
