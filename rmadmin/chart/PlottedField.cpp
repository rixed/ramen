#include "PlottedField.h"

PlottedField::PlottedField(
    std::shared_ptr<Function> function_, int column_, std::vector<int> factors_,
    Representation repr)
  : function(function_),
    column(column_),
    factors(factors_),
    representation(repr)
{}

void PlottedField::paint(QWidget *) const
{
}
