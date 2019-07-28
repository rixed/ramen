#ifndef RANGEDOUBLEVALIDATOR_H_190504
#define RANGEDOUBLEVALIDATOR_H_190504
#include <QDoubleValidator>

namespace RangeDoubleValidator {

QDoubleValidator const *forRange(double min, double max);

};

#endif
