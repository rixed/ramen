#include <cassert>
#include <QDebug>
#include "confValue.h"
#include "AtomicWidgetAlternative.h"

static bool const verbose(false);

AtomicWidgetAlternative::AtomicWidgetAlternative(QWidget *parent) :
  AtomicWidget(parent), currentWidget(-1) {}

int AtomicWidgetAlternative::addWidget(AtomicWidget *w)
{
  if (verbose)
    qDebug() << "AtomicWidgetAlternative: adding widget" << w
             << "as" << widgets.size();

  widgets.push_back(w);
  int i = widgets.size() - 1;
  if (currentWidget < 0) currentWidget = i;
  return i;
}

void AtomicWidgetAlternative::setEnabled(bool enabled)
{
  for (AtomicWidget *w : widgets)
    w->setEnabled(enabled);
}

std::shared_ptr<conf::Value const> AtomicWidgetAlternative::getValue() const
{
  assert(currentWidget >= 0);
  return widgets[currentWidget]->getValue();
}

bool AtomicWidgetAlternative::setValue(
  std::string const &k, std::shared_ptr<conf::Value const>v)
{
  assert(currentWidget >= 0);
  return widgets[currentWidget]->setValue(k, v);
}

void AtomicWidgetAlternative::setCurrentWidget(int i)
{
  assert(i >= 0 && i < (int)widgets.size());

  if (verbose)
    qDebug() << "AtomicWidgetAlternative: current widget is now"
             << i << "(" << this << ")";

  currentWidget = i;
}

std::string const &AtomicWidgetAlternative::key() const
{
  assert(currentWidget >= 0);
  return widgets[currentWidget]->key();
}

void AtomicWidgetAlternative::saveKey(std::string const &newKey)
{
  assert(currentWidget >= 0);
  widgets[currentWidget]->saveKey(newKey);
}

bool AtomicWidgetAlternative::setKey(std::string const &newKey)
{
  if (verbose)
    qDebug() << "AtomicWidgetAlternative::setKey("
             << QString::fromStdString(newKey) << ")";

  bool const ok(AtomicWidget::setKey(newKey));
  assert(currentWidget >= 0);
  if (ok)
    widgets[currentWidget]->setKey(newKey);

  return ok;
}
