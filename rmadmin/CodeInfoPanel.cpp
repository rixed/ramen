#include "CodeInfoPanel.h"

void CodeInfoPanel::setValue(conf::Key const &key, std::shared_ptr<conf::Value const> v)
{
  (void)key;
  value = v;
  // TODO
}
