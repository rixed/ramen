#include <iostream>
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

  std::cout << "Creating a new validator for ints between "
            << min << " and " << max << std::endl;

  // 1000 decimal digits is the default:
  QIntValidator *validator = new QIntValidator(min, max);
  validators.push_back(validator);
  return validator;
}

};
