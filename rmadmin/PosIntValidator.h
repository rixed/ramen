#ifndef POSINTVALIDATOR_H_190504
#define POSINTVALIDATOR_H_190504
#include <QIntValidator>

class PosIntValidator : public QIntValidator
{
  Q_OBJECT

public:
  PosIntValidator(QObject *parent = nullptr) : QIntValidator(parent)
  {
    setBottom(0);
  }
  ~PosIntValidator() {}
};

extern PosIntValidator posIntValidator;

#endif
