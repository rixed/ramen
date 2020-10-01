#include <QtGlobal>
#include <QDebug>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QFormLayout>
#include <QTableWidget>
#include <QHeaderView>
#include <QLabel>
#include <QTabWidget>
#include "misc.h"
#include "CompiledProgramParam.h"
#include "CompiledFunctionInfo.h"
#include "RamenType.h"
#include "DessserValueType.h"
#include "confValue.h"
#include "SourceInfoViewer.h"

SourceInfoViewer::SourceInfoViewer(QWidget *parent) :
  AtomicWidget(parent)
{
  layout = new QVBoxLayout;
  QWidget *contents = new QWidget;
  contents->setObjectName("infoViewerContents");
  contents->setLayout(layout);
  contents->setMinimumHeight(400);
  relayoutWidget(contents);
}

bool SourceInfoViewer::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v)
{
  /* Empty the previous params/parents layouts: */
  /* FIXME: rather instanciate the widgets only once and hide those unused */
  emptyLayout(layout);

  std::shared_ptr<conf::SourceInfo const> i =
    std::dynamic_pointer_cast<conf::SourceInfo const>(v);
  if (i) {
    if (i->errMsg.length() > 0) {
      QLabel *l = new QLabel(i->errMsg);
      l->setWordWrap(true);
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
          QLabel *l = new QLabel(
            p->val ? p->val->toQString(std::string()) : "NULL");
          l->setWordWrap(true);
          paramsLayout->addRow(QString::fromStdString(p->name + ":"), l);
          if (p->doc.size() > 0) {
            QLabel *doc = new QLabel(QString::fromStdString(p->doc));
            doc->setWordWrap(true);
            paramsLayout->addRow(doc);
          }
        }
        layout->addLayout(paramsLayout);
      }

      QTabWidget *functions = new QTabWidget;
      for (auto &func : i->infos) {
        QVBoxLayout *l = new QVBoxLayout;
        QWidget *w = new QWidget;
        w->setLayout(l);
        QString title =
          QString(func->name + (func->is_lazy ? " (lazy)" : ""));
        functions->addTab(w, title);
        if (func->doc.length() > 0) {
          QLabel *doc = new QLabel(func->doc);
          doc->setWordWrap(true);
          l->addWidget(doc);
        }
        QLabel *retention = new QLabel(
          tr("Retention: ") +
          (func->retention ?
            func->retention->toQString(std::string()) :
            "<i>" + tr("none") + "</i>"));
        l->addWidget(retention);

        l->addWidget(new QLabel(tr("Output Type:")));
        // One line per "column" of the outType:
        QTableWidget *columns = new QTableWidget;
        l->addWidget(columns);
        columns->setColumnCount(3);
        columns->setHorizontalHeaderLabels({ "Name", "Type", "Low Card." });
        columns->setEditTriggers(QAbstractItemView::NoEditTriggers);
        columns->verticalHeader()->setVisible(false);
        unsigned numColumns(func->outType->vtyp->numColumns());
        columns->setRowCount(numColumns);

        for (unsigned c = 0; c < numColumns; c ++) {
          QString const name(func->outType->vtyp->columnName(c));
          bool const isFactor = func->factors.contains(name);

          std::shared_ptr<RamenType const> subtype =
            func->outType->vtyp->columnType(c);
          columns->setItem(c, 0, new QTableWidgetItem(name));
          columns->setItem(c, 1, new QTableWidgetItem(
            subtype ?
              subtype->toQString() :
              func->outType->toQString()));
          columns->setItem(c, 2, new QTableWidgetItem(isFactor ? "✓":""));
        }
        columns->resizeColumnsToContents();

        QLabel *sign = new QLabel(tr("Signature: %1").arg(func->signature));
        l->addWidget(sign);
      }
      layout->addWidget(new QLabel("<b>" + tr("Functions") + "</b>"));
      layout->addWidget(functions);
    }
    layout->addSpacing(10);
    QLabel *md5 = new QLabel("For sources which MD5 are " + i->md5s.join(","));
    md5->setWordWrap(true);
    layout->addWidget(md5);
    return true;
  } else {
    qCritical() << "Not a SourceInfo value?!";
    return false;
  }
}
