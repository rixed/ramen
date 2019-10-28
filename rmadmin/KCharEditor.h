#ifndef KCHAREDITOR_H_191029
#define KCHAREDITOR_H_191029
#include "KIntEditor.h"

class KCharEditor : public KIntEditor {

public:
  KCharEditor(std::function<RamenValue *(QString const &)> v,
              QWidget *parent = nullptr) :
    KIntEditor(v, parent, std::numeric_limits<char>::min(), std::numeric_limits<char>::max()) {}
};

#endif
