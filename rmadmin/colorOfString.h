#ifndef COLOROFSTRING_H_190512
#define COLOROFSTRING_H_190512
#include <QColor>

/* The idea is clear:
 * Generate a random color from a string, so that the same string is
 * always associated with the same color, and therefore can be used to
 * represent a site, a program or a function.
 * Despite this, this try to return as different colors as possible,
 * thus the paletteSize parameter. Also, when the paletteSize changes
 * the generated colors does not change too much.
 */

extern unsigned paletteSize;

QColor colorOfString(QString const &);

#endif
