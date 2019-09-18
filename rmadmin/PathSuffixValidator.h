#ifndef PATHSUFFIXVALIDATOR_H_190810
#define PATHSUFFIXVALIDATOR_H_190810
#include <QValidator>
/* Validator for program name suffixes, which must contain no '/'. */

class PathSuffixValidator : public QValidator
{
  Q_OBJECT

public:
  PathSuffixValidator(QObject *parent = nullptr) :
    QValidator(parent) {}

  QValidator::State validate(QString &, int &) const;
};

#endif
