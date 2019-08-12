#ifndef PATHNAMEVALIDATOR_H_190810
#define PATHNAMEVALIDATOR_H_190810
#include <QValidator>

class PathNameValidator : public QValidator
{
  Q_OBJECT

public:
  PathNameValidator(QObject *parent = nullptr) :
    QValidator(parent) {}

  QValidator::State validate(QString &, int &) const;
};

#endif
