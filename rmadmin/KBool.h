#ifndef KBOOL_H_190516
#define KBOOL_H_190516
#include "KChoice.h"

class KBool : public KChoice
{
  Q_OBJECT

public:
  KBool(std::string const key, QString const &yesLabel, QString const &noLabel, QWidget *parent = nullptr);
};

#endif
