#ifndef RAMENVALUEEDITOR_H_190619
#define RAMENVALUEEDITOR_H_190619
#include <QLineEdit>
#include "RamenValue.h"

/* For now, just a QLineEdit with whatever additional methods as
 * required by RCEntryEditor. TODO: type specific editor.
 * Note: not an AtomicWidget as those requires a config key but
 * we might have none. It's still possible to combine one or several
 * RamenValueEditor into a larger AtomicWidget though. */

class RamenValueEditor : public QLineEdit
{
  Q_OBJECT

  // FIXME: rename into structure
  enum RamenTypeStructure type;

public:
  RamenValueEditor(enum RamenTypeStructure type_, QWidget *parent = nullptr) :
    QLineEdit(parent),
    type(type_) {}

  virtual ~RamenValueEditor() {}

  // Caller takes ownership
  RamenValue *getValue() const;

  static RamenValueEditor *ofType(enum RamenTypeStructure, RamenValue const *, QWidget *parent = nullptr);
};

#endif
