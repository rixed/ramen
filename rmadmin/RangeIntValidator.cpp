#include <vector>
#include "RangeIntValidator.h"

namespace RangeIntValidator {

static std::vector<QIntValidator *> validators;

QIntValidator const *forRange(int min, int max)
{
  // Look for a previously created validator for that range:
  for (auto const validator : validators) {
    if (validator->bottom() == min && validator->top() == max)
      return validator;
  }

  QIntValidator *validator = new QIntValidator(min, max);
  validators.push_back(validator);
  return validator;
}

};
