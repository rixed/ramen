#ifndef KBOOL_H_190516
#define KBOOL_H_190516
#include "KChoice.h"

class KBool : public KChoice
{
  Q_OBJECT

public:
  KBool(conf::Key const &key, QString const &yesLabel, QString const &noLabel, QWidget *parent = nullptr);
  KBool(conf::Key const &key, QWidget *parent = nullptr);
};

#endif
