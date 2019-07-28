#include <QLabel>
#include <QFormLayout>
#include <QTableWidget>
#include "ProgramItem.h"
#include "KLineEdit.h"
#include "KFloatEditor.h"
#include "KLabel.h"
#include "KBool.h"
#include "ProgramInfoBox.h"

/* FIXME: This is uterly broken since we have no more programs/.* keys.
 * Use source info for the program instead. */

ProgramInfoBox::ProgramInfoBox(ProgramItem const *p_, QWidget *parent) :
  AtomicForm(p_->name, parent),
  p(p_),
  pref("programs/" + p->name.toStdString() + "/")
{
  QFormLayout *layout = new QFormLayout;
  QWidget *cw = new QWidget(this);
  cw->setLayout(layout);
  setCentralWidget(cw);

  KBool *enabled =
    new KBool(conf::Key(pref + "enabled"), tr("run that program"), tr("do not run"));
  layout->addRow(tr("&Enabled"), enabled);
  addWidget(enabled);

  KBool *debug =
    new KBool(conf::Key(pref + "debug"), tr("normal"), tr("verbose logs"));
  layout->addRow(tr("&Debug"), debug);
  addWidget(debug);

  KFloatEditor *reportPeriod =
    new KFloatEditor(conf::Key(pref + "report_period"), cw);
  layout->addRow(tr("&Reporting Interval"), reportPeriod);
  addWidget(reportPeriod);

  KLabel *binPath = new KLabel(conf::Key(pref + "bin_path"), false, cw);
  layout->addRow(tr("Executable"), binPath);

  KLabel *srcPath = new KLabel(conf::Key(pref + "src_path"), false, cw);
  layout->addRow(tr("Source File"), srcPath);

  paramTable = new QTableWidget(this);
  paramTable->setColumnCount(2);
  paramTable->setHorizontalHeaderLabels({ "Name", "Value" });
  std::string k("^" + pref + "param/");
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
    // We only need creation/destruction as the AtomicWidget will take
    // care of the rest:
    connect(kv, &KValue::valueCreated, this, &ProgramInfoBox::setParam);
    connect(kv, &KValue::valueDeleted, this, &ProgramInfoBox::delParam);
  });
  layout->addRow(tr("Parameters"), paramTable);

  KLineEdit *onSites = new KLineEdit(conf::Key(pref + "on_site"), cw);
  layout->addRow(tr("Run &On Sites"), onSites);
  addWidget(onSites);

  KLabel *automatic = new KLabel(conf::Key(pref + "automatic"), false, cw);
  layout->addRow(tr("Automatic"), automatic);

  KLabel *runCondition = new KLabel(conf::Key(pref + "run_condition"), false, cw);
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

void ProgramInfoBox::setParam(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString name = paramOfKey(k, pref);
  if (name.isNull()) return;

  int row = findRow(paramTable, name);

  if (row >= paramTable->rowCount()) {
    paramTable->setRowCount(row + 1);
    paramTable->setItem(row, 0, new QTableWidgetItem(name));
  }

  AtomicWidget *widget = v->editorWidget(k);
  paramTable->setCellWidget(row, 1, widget);
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
