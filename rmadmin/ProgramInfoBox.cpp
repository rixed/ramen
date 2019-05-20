#include <QLabel>
#include <QFormLayout>
#include <QTableWidget>
#include "ProgramItem.h"
#include "KLineEdit.h"
#include "KLabel.h"
#include "KBool.h"
#include "ProgramInfoBox.h"

ProgramInfoBox::ProgramInfoBox(ProgramItem const *p_, QWidget *parent) :
  AtomicForm(p_->name, parent),
  p(p_),
  pref("programs/" + p->name.toStdString() + "/")
{
  QFormLayout *layout = new QFormLayout;
  QWidget *cw = new QWidget(this);
  cw->setLayout(layout);
  setCentralWidget(cw);

  KBool *mustRun =
    new KBool(pref + "must_run", tr("run that program"), tr("do not run"));
  layout->addRow(tr("&Enabled"), mustRun);
  addWidget(mustRun);

  KBool *debug =
    new KBool(pref + "debug", tr("normal"), tr("verbose logs"));
  layout->addRow(tr("&Debug"), debug);
  addWidget(debug);

  KLineEdit *reportPeriod =
    new KLineEdit(pref + "report_period", conf::ValueType::FloatType, cw);
  layout->addRow(tr("&Reporting Interval"), reportPeriod);
  addWidget(reportPeriod);

  KLabel *binPath = new KLabel(pref + "bin_path", cw);
  layout->addRow(tr("Executable"), binPath);

  KLabel *srcPath = new KLabel(pref + "src_path", cw);
  layout->addRow(tr("Source File"), srcPath);

  paramTable = new QTableWidget(this);
  paramTable->setColumnCount(2);
  paramTable->setHorizontalHeaderLabels({ "Name", "Value" });
  std::string k(pref + "param/");
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
    // We only need creation/destruction as the AtomicWidget will take
    // care of the rest:
    QObject::connect(kv, &KValue::valueCreated, this, &ProgramInfoBox::setParam);
    QObject::connect(kv, &KValue::valueDeleted, this, &ProgramInfoBox::delParam);
  });
  layout->addRow(tr("Parameters"), paramTable);

  KLineEdit *onSites =
    new KLineEdit(pref + "on_site", conf::ValueType::StringType, cw);
  layout->addRow(tr("Run &On Sites"), onSites);
  addWidget(onSites);

  KLabel *automatic = new KLabel(pref + "automatic", cw);
  layout->addRow(tr("Automatic"), automatic);

  KLabel *runCondition = new KLabel(pref + "run_condition", cw);
  layout->addRow(tr("Condition to Run"), runCondition);
}

static QString paramOfKey(conf::Key const &k, std::string pref)
{
  size_t c;
  for (c = 0; c < pref.length(); c++) {
    if (k.s[c] != pref[c]) {
      std::cout << "set/delParam on key " << k.s << " not prefixed with " << pref << std::endl;
      return QString();
    }
  }

  return QString::fromStdString(k.s.substr(c));
}

static int findRow(QTableWidget const *table, QString const &name)
{
  int row;
  for (row = 0; row < table->rowCount(); row++) {
    QTableWidgetItem const *w = table->item(row, 0);
    if (w->text() == name) break;
  }
  return row;
}

void ProgramInfoBox::setParam(conf::Key const &k, std::shared_ptr<conf::Value const>)
{
  QString name = paramOfKey(k, pref);
  if (name.isNull()) return;

  int row = findRow(paramTable, name);

  if (row >= paramTable->rowCount()) {
    paramTable->setRowCount(row + 1);
    paramTable->setItem(row, 0, new QTableWidgetItem(name));
  }

  KLineEdit *kle =
    // TODO: the param type should be passed as well (in the same value,
    // ie the conf value must be of type "RamenValue"), and we should
    // have a ValueType for each possible RamenType, and a function
    // selecting the appropriate one and use it here instead of
    // hardcoding StringType:
    new KLineEdit(k.s, conf::ValueType::StringType);
  paramTable->setCellWidget(row, 1, kle);
}

void ProgramInfoBox::delParam(conf::Key const &k)
{
  QString name = paramOfKey(k, pref);
  if (name.isNull()) return;

  int row = findRow(paramTable, name);
  if (row < paramTable->rowCount()) {
    paramTable->removeRow(row);
  } else {
    std::cout << "deletion of unknown param " << k.s << ", good riddance!" << std::endl;
  }
}
