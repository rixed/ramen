#ifndef RANGEINTVALIDATOR_H_190504
#define RANGEINTVALIDATOR_H_190504
#include <QIntValidator>

namespace RangeIntValidator {

QIntValidator const *forRange(int min, int max);

};

#endif
