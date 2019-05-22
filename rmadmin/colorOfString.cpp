#include <cassert>
#include <cmath>
#include "colorOfString.h"

static unsigned rescale(unsigned x, unsigned from, unsigned to)
{
  assert(x < from);
  return ((double)x / from) * to;
}

static unsigned to256(unsigned x, unsigned from)
{
  return rescale(x, from, 256);
}

unsigned paletteSize = 80;

QColor colorOfString(QString const &s)
{
  unsigned numCat = ceil(std::pow(paletteSize, 1./3));
  uint64_t col = qHash(s);

  unsigned r = to256(col % numCat, numCat);
  col /= numCat;
  unsigned g = to256(col % numCat, numCat);
  col /= numCat;
  unsigned b = to256(col % numCat, numCat);

  return QColor(r, g, b);
}
