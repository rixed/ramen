#include "PathNameValidator.h"

/* Not empty, not "." or "..", no heading or trailing slash, not starting with
 * "./" or "../", not ending with "/." or "/..", and containing no "/./" or
 * "/../". See RamenName.program. */
QValidator::State PathNameValidator::validate(QString &input, int &) const
{
  if (input.contains("/./") || input.contains("/../") ||
      input.startsWith('/') ||
      input.startsWith("./") || input.startsWith("../"))
    return QValidator::Invalid;

  if ("" == input || "." == input || ".." == input ||
      input.endsWith('/') ||
      input.endsWith("/.") || input.endsWith("/.."))
    return QValidator::Intermediate;

  return QValidator::Acceptable;
}
