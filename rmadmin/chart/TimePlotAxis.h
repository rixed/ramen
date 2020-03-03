#ifndef TIMEPLOTAXIS_H_200303
#define TIMEPLOTAXIS_H_200303

class QWidget;

class TimePlotAxis
{
  enum Side { LEFT, RIGHT } side;

  bool forceZero;

  enum Scale { LIN, LOG } scale;

  enum LabelFormat { Decimal } labelFormat;

public:
  TimePlotAxis(Side, bool forceZero, Scale);

  void paint(QWidget *) const;
};

#endif
