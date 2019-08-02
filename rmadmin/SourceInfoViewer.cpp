#include <iostream>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QFormLayout>
#include <QLabel>
#include <QToolBox>
#include "misc.h"
#include "once.h"
#include "CompiledProgramParam.h"
#include "CompiledFunctionInfo.h"
#include "RamenType.h"
#include "RamenTypeStructure.h"
#include "SourceInfoViewer.h"

SourceInfoViewer::SourceInfoViewer(conf::Key const &k, QWidget *parent) :
  AtomicWidget(k, parent)
{
  layout = new QVBoxLayout;
  QWidget *w = new QWidget;
  w->setLayout(layout);
  w->setMinimumHeight(650);
  setCentralWidget(w);

  SET_INITIAL_VALUE;

  Once::connect(&kv, &KValue::valueCreated, this, &SourceInfoViewer::setValue);
  connect(&kv, &KValue::valueChanged, this, &SourceInfoViewer::setValue);
  // del?
}

bool SourceInfoViewer::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  /* Empty the previous params/parents layouts: */
  emptyLayout(layout);

  std::shared_ptr<conf::SourceInfo const> i =
    std::dynamic_pointer_cast<conf::SourceInfo const>(v);
  if (i) {
    if (i->errMsg.length() > 0) {
      QLabel *l = new QLabel(i->errMsg);
      l->setAlignment(Qt::AlignCenter);
      layout->addWidget(l);
    } else {
      layout->addWidget(new QLabel("<b>" + tr("Parameters") + "</b>"));

      if (i->params.size() == 0) {
        QLabel *none = new QLabel("<i>" + tr("none") + "</i>");
        none->setAlignment(Qt::AlignCenter);
        layout->addWidget(none);
      } else {
        QFormLayout *paramsLayout = new QFormLayout;
        for (auto &p : i->params) {
          paramsLayout->addRow(QString::fromStdString(p.name + ":"),
                              new QLabel(p.val ? p.val->toQString() : "NULL"));
          if (p.doc.size() > 0)
            paramsLayout->addRow(new QLabel(QString::fromStdString(p.doc)));
        }
      }

      QToolBox *functions = new QToolBox;
      for (auto &func : i->infos) {
        QVBoxLayout *l = new QVBoxLayout;
        QWidget *w = new QWidget;
        w->setLayout(l);
        QString title =
          QString(func.name + (func.is_lazy ? " (lazy)" : ""));
        functions->addItem(w, title);
        if (func.doc.length() > 0)
          l->addWidget(
            new QLabel(func.doc));
        QLabel *retention = new QLabel(
          tr("Retention: ") +
          (func.retention ?
            func.retention->toQString() :
            "<i>" + tr("none") + "</i>"));
        l->addWidget(retention);

        l->addWidget(new QLabel(tr("Output Type:")));
        // One line per "column" of the out_type:
        QFormLayout *columns = new QFormLayout;
        l->addLayout(columns);
        for (unsigned c = 0; c < func.out_type->structure->numColumns(); c ++) {
          QString const name(func.out_type->structure->columnName(c));
          bool const isFactor = func.factors.contains(name);

          std::shared_ptr<RamenType const> subtype =
            func.out_type->structure->columnType(c);
          columns->addRow(
            name + (isFactor ? " [FACTOR]:" : ":"),
            new QLabel(
              subtype ?
                subtype->toQString() :
                func.out_type->toQString()));
        }

        QLabel *sign = new QLabel(tr("Signature: %1").arg(func.signature));
        l->addWidget(sign);
      }
      layout->addWidget(new QLabel("<b>" + tr("Functions") + "</b>"));
      layout->addWidget(functions);
    }
    layout->addSpacing(10);
    layout->addWidget(new QLabel("For source which MD5=" + i->md5));
    return true;
  } else {
    std::cerr << "Not a SourceInfo value?!" << std::endl;
    return false;
  }
}
