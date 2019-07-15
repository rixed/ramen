#include <cassert>
#include <QRadioButton>
#include <QVBoxLayout>
#include "KChoice.h"

KChoice::KChoice(std::string const key, std::vector<std::pair<QString const, std::shared_ptr<conf::Value const>>> labels, QWidget *parent) :
  QWidget(parent),
  AtomicWidget(key)
{
  QVBoxLayout *layout = new QVBoxLayout;
  setLayout(layout);

  for (auto label : labels) {
    QRadioButton *b = new QRadioButton(label.first, this);
    choices.push_back({b, label.second});
    layout->addWidget(b);
  }

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();
  connect(&kv, &KValue::valueCreated, this, &KChoice::setValue);
  connect(&kv, &KValue::valueChanged, this, &KChoice::setValue);
  connect(&kv, &KValue::valueLocked, this, &KChoice::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KChoice::unlockValue);
  if (kv.isSet()) setValue(key, kv.val);
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

void KChoice::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  for (unsigned i = 0; i < choices.size(); i ++) {
    if (*choices[i].second == *v) {
      choices[i].first->setChecked(true);
      return;
    }
  }

  std::cout << "No choice correspond to value " << *v << std::endl;
}

void KChoice::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  for (auto c : choices) {
    c.first->setEnabled(enabled);
  }
}
