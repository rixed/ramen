#include <iostream>
#include <vector>
#include "RangeDoubleValidator.h"

namespace RangeDoubleValidator {

static std::vector<QDoubleValidator *> validators;

QDoubleValidator const *forRange(double min, double max)
{
  // Look for a previously created validator for that range:
  for (auto const validator : validators) {
    if (validator->bottom() == min && validator->top() == max)
      return validator;
  }

  std::cout << "Creating a new validator for doubles between "
            << min << " and " << max << std::endl;

  // 1000 decimal digits is the default:
  QDoubleValidator *validator = new QDoubleValidator(min, max, 1000);
  validators.push_back(validator);
  return validator;
}

};
