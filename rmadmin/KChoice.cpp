#include <cassert>
#include <QRadioButton>
#include <QVBoxLayout>
#include "once.h"
#include "KChoice.h"

KChoice::KChoice(std::vector<std::pair<QString const, std::shared_ptr<conf::Value const>>> labels, QWidget *parent) :
  AtomicWidget(parent)
{
  widget = new QWidget;
  QVBoxLayout *layout = new QVBoxLayout;
  widget->setLayout(layout);
  setCentralWidget(widget);

  for (auto label : labels) {
    QRadioButton *b = new QRadioButton(label.first);
    choices.push_back({b, label.second});
    layout->addWidget(b);

    connect(b, &QRadioButton::released,
            this, &KChoice::inputChanged);
  }

}

void KChoice::extraConnections(KValue *kv)
{
  Once::connect(kv, &KValue::valueCreated, this, &KChoice::setValue);
  connect(kv, &KValue::valueChanged, this, &KChoice::setValue);
  connect(kv, &KValue::valueLocked, this, &KChoice::lockValue);
  connect(kv, &KValue::valueUnlocked, this, &KChoice::unlockValue);
}

std::shared_ptr<conf::Value const> KChoice::getValue() const
{
  for (size_t i = 0; i < choices.size(); i++) {
    if (choices[i].first->isChecked())
      return choices[i].second;
  }

  std::cout << "No radio button checked!?" << std::endl;
  return choices[0].second;
}

bool KChoice::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
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

  std::cout << "No choice correspond to value " << *v << std::endl;
  return false;
}

void KChoice::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  for (auto c : choices) {
    c.first->setEnabled(enabled);
  }
}
