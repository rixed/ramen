#include "PathSuffixValidator.h"

/* No dots and no slashs. May be empty. */
QValidator::State PathSuffixValidator::validate(QString &input, int &) const
{
  if (input.contains('.') || input.contains('/'))
    return QValidator::Invalid;

  return QValidator::Acceptable;
}
