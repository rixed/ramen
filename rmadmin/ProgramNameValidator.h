#ifndef PROGRAMNAMEVALIDATOR_H_190810
#define PROGRAMNAMEVALIDATOR_H_190810
#include <QValidator>

class ProgramNameValidator : public QValidator
{
  Q_OBJECT

public:
  ProgramNameValidator(QObject *parent = nullptr) :
    QValidator(parent) {}

  QValidator::State validate(QString &, int &) const;
};

#endif
