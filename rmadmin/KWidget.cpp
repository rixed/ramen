#include "KWidget.h"
#include "conf.h"

KWidget::KWidget(std::string const key_) :
    key(key_)
{
  conf::registerWidget(key_, this);
}

KWidget::~KWidget()
{
  if (key) conf::unregisterWidget(*key, this);
}
