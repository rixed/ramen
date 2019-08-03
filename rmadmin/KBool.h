#ifndef KBOOL_H_190516
#define KBOOL_H_190516
#include "KChoice.h"

class KBool : public KChoice
{
  Q_OBJECT

public:
  KBool(QString const &yesLabel, QString const &noLabel, QWidget *parent = nullptr);
  KBool(QWidget *parent = nullptr);
};

#endif
