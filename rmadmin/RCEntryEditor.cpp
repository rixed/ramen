#include <cassert>
#include <string>
#include <memory>
#include <iostream>
#include <QFormLayout>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QCheckBox>
#include <QComboBox>
#include <QLabel>
#include "conf.h"
#include "misc.h"
#include "once.h"
#include "SourcesModel.h"  // for baseNameOfKey and friends
#include "RangeDoubleValidator.h"
#include "PathNameValidator.h"
#include "confRCEntryParam.h"
#include "AtomicWidget.h"
#include "RCEntryEditor.h"

static bool verbose = false;

QMap<std::string, std::shared_ptr<RamenValue const>> RCEntryEditor::setParamValues;

static bool isCompiledSource(KValue const &kv)
{
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.val);
  if (! info) return false;

  return info->isInfo();
}

static bool isSourceFile(std::string const &key)
{
  return startsWith(key, "sources/") && ! endsWith(key, "/info");
}

static bool isInfoFile(std::string const &key)
{
  return startsWith(key, "sources/") && endsWith(key, "/info");
}

RCEntryEditor::RCEntryEditor(bool sourceEditable_, QWidget *parent) :
  QWidget(parent),
  sourceEditable(sourceEditable_)
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  nameEdit = new QLineEdit;
  nameEdit->setPlaceholderText("Unique name");
  nameEdit->setValidator(new PathNameValidator(this));
  layout->addRow(tr("Program &Name:"), nameEdit);
  connect(nameEdit, &QLineEdit::textChanged,
          this, &RCEntryEditor::inputChanged);

  { // source

    QVBoxLayout *sourceLayout = new QVBoxLayout;
    sourceBox = new QComboBox;
    sourceBox->setEnabled(sourceEditable);

    sourceLayout->addWidget(sourceBox);

    connect(sourceBox, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, [this](int) {
      updateSourceWarnings();
      resetParams();
      emit inputChanged();
    });

    notCompiledSourceWarning =
      new QLabel(tr("This source is not compiled (yet?)"));
    notCompiledSourceWarning->setVisible(false);
    notCompiledSourceWarning->setStyleSheet("color: red;");
    sourceLayout->addWidget(notCompiledSourceWarning);
    deletedSourceWarning =
      new QLabel(tr("This source does not exist any longer!"));
    sourceLayout->addWidget(deletedSourceWarning);
    deletedSourceWarning->setVisible(false);
    deletedSourceWarning->setStyleSheet("color: red;");

    layout->addRow(tr("Source:"), sourceLayout);
  }

  { // flags
    QVBoxLayout *flagsLayout = new QVBoxLayout;
    enabledBox = new QCheckBox(tr("enabled"));
    enabledBox->setChecked(true);
    connect(enabledBox, &QCheckBox::stateChanged,
            this, &RCEntryEditor::inputChanged);
    flagsLayout->addWidget(enabledBox);
    debugBox = new QCheckBox(tr("debug mode"));
    flagsLayout->addWidget(debugBox);
    connect(debugBox, &QCheckBox::stateChanged,
            this, &RCEntryEditor::inputChanged);
    automaticBox = new QCheckBox(tr("automatic"));
    automaticBox->setEnabled(false);
    flagsLayout->addWidget(automaticBox);
    layout->addRow(tr("Flags:"), flagsLayout);
  }

  sitesEdit = new QLineEdit;
  sitesEdit->setText("*");
  static QRegularExpression nonEmpty("\\s*[^\\s]+\\s*");
  sitesEdit->setValidator(new QRegularExpressionValidator(nonEmpty, this));
  connect(sitesEdit, &QLineEdit::textChanged,
          this, &RCEntryEditor::inputChanged);
  layout->addRow(tr("&Only on Sites:"), sitesEdit);

  reportEdit = new QLineEdit;
  reportEdit->setText("30");
  reportEdit->setValidator(RangeDoubleValidator::forRange(1, 99999));
  connect(reportEdit, &QLineEdit::textChanged,
          this, &RCEntryEditor::inputChanged);
  layout->addRow(tr("&Reporting Period:"), reportEdit);

  paramsForm = new QFormLayout;
  layout->addRow(new QLabel(tr("Parameters:")));
  layout->addRow(paramsForm);

  /* Each creation/deletion of source files while this editor is alive should
   * refresh the source select box and its associated warnings: */
  conf::autoconnect("^sources/.*", [this](conf::Key const &, KValue const *kv) {
    Once::connect(kv, &KValue::valueCreated, this, [this](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      if (isSourceFile(k.s)) {
        if (verbose) std::cout << "New source key: " << k.s << std::endl;
        addSource(k); // Won't change the selection but might change warnings
        updateSourceWarnings();
      } else if (isInfoFile(k.s)) {
        updateSourceWarnings();
      }
    });
    connect(kv, &KValue::valueDeleted, this, [this](conf::Key const &) {
      /* Do not remove anything, so keep the current selection.
       * Just update the warning: */
      updateSourceWarnings();
    });
  });
}

void RCEntryEditor::setSourceName(QString const &baseName)
{
  /* Even if this sourceName does not exist we want to have it preselected
   * (with a warning displayed).
   * If several non-existing sourceNames are set, they will accumulate in
   * the box. Not a big deal as long as the warnings are properly displayed. */
  int index = addSourceName(baseName);
  if (index >= 0) sourceBox->setCurrentIndex(index);
}

int RCEntryEditor::addSource(conf::Key const &k)
{
  return addSourceName(baseNameOfKey(k));
}

int RCEntryEditor::addSourceName(QString const &name)
{
  // Insert in alphabetic order:
  for (int i = 0; i <= sourceBox->count(); i ++) {
    QString const current = sourceBox->itemText(i);
    // Do not add it once more (can happen when it was preselected)
    if (current == name) return i;
    if (i == sourceBox->count() || name < current) {
      sourceBox->insertItem(i, name);
      return i;
    }
  }

  assert(!"Hit by a gamma rays!");
  return -1;
}

void RCEntryEditor::updateSourceWarnings()
{
  int i = sourceBox->currentIndex();
  if (i < 0 || i > sourceBox->count()) {
    /* No need to warn about the selected entry, the placeholder
     * text is enough. */
    sourceDoesExist = true;
    sourceIsCompiled = true;
  } else {
    QString const name(sourceBox->currentText());
    conf::Key info_k(keyOfSourceName(name, "info"));
    conf::kvs_lock.lock_shared();
    sourceDoesExist =
      conf::kvs.contains(keyOfSourceName(name, "ramen")) ||
      conf::kvs.contains(keyOfSourceName(name, "alert"));
    sourceIsCompiled =
      conf::kvs.contains(info_k) &&
      isCompiledSource(conf::kvs[info_k]);
    conf::kvs_lock.unlock_shared();
  }

  deletedSourceWarning->setVisible(!sourceDoesExist);
  notCompiledSourceWarning->setVisible(!sourceIsCompiled);
}

void RCEntryEditor::clearParams()
{
  while (paramsForm->rowCount() > 0)
    paramsForm->removeRow(0); // Note: this also deletes the widgets
}

std::shared_ptr<RamenValue const> RCEntryEditor::paramValue(CompiledProgramParam const *p) const
{
  /* Try to find a set parameter by that name, falling back on the
   * compiled default: */
  if (verbose)
    std::cout << "paramValue(" << p->name << ") is "
              << (setParamValues.contains(p->name) ? "present" : "absent")
              << std::endl;
  return setParamValues.value(p->name, p->val);
}

void RCEntryEditor::resetParams()
{
  /* Clear the paramsForm and rebuilt it.
   * But first, save the values that are currently set (and that can be
   * parsed) into setParamValues, for later reuse. */
  for (int row = 0; row < paramsForm->rowCount(); row ++) {
    QLayoutItem *item = paramsForm->itemAt(row, QFormLayout::LabelRole);
    QLabel *label = dynamic_cast<QLabel *>(item->widget());
    assert(label);
    std::string const pname(label->text().toStdString());
    item = paramsForm->itemAt(row, QFormLayout::FieldRole);
    AtomicWidget *editor = dynamic_cast<AtomicWidget *>(item->widget());
    assert(editor);
    std::shared_ptr<conf::RamenValueValue const> rval =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(editor->getValue());
    if (rval) {
      if (verbose)
        std::cout << "set paramValues[" << pname << "]" << std::endl;
      setParamValues[pname] = rval->v;
    } else {
      std::cerr << "AtomicWidget editor returned a confValue for row " << row
                << " (name " << pname << ") that's not a RamenValueValue!?"
                << std::endl;
    }
  }

  QString const baseName = removeExtQ(sourceBox->currentText());
  conf::Key infoKey("sources/" + baseName.toStdString() + "/info");

  conf::kvs_lock.lock_shared();
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(conf::kvs[infoKey].val);
  conf::kvs_lock.unlock_shared();

  if (! info) {
    std::cerr << "Cannot get info for " << baseName.toStdString() << std::endl;
    return;
  }

  clearParams();

  for (unsigned i = 0; i < info->params.size(); i ++) {
    CompiledProgramParam const *p = &info->params[i];
    // TODO: a tooltip with the parameter doc (CompiledProgramParam doc)
    std::shared_ptr<RamenValue const> val = paramValue(p);
    AtomicWidget *paramEdit = val->editorWidget(conf::Key::null);
    /* In theory, AtomicWidget got their value from the key. But here we
     * have no key but we know the value so let's just set it: */
    std::shared_ptr<conf::RamenValueValue const> confval =
      std::make_shared<conf::RamenValueValue const>(val);
    paramEdit->setValue(conf::Key::null, std::static_pointer_cast<conf::Value const>(confval));
    paramEdit->setEnabled(true);
    connect(paramEdit, &AtomicWidget::inputChanged,
            this, &RCEntryEditor::inputChanged);
    paramsForm->addRow(QString::fromStdString(p->name), paramEdit);
  }
}

void RCEntryEditor::setValue(conf::RCEntry const *rcEntry)
{
  nameEdit->setText(QString::fromStdString(rcEntry->programName));

  if (rcEntry->source.length() == 0) {
    sourceBox->setEnabled(false);
  } else {
    sourceBox->setEnabled(sourceEditable);
    QString source = QString::fromStdString(rcEntry->source);
    int i;
    for (i = 0; i < sourceBox->count(); i++) {
      if (sourceBox->itemText(i) == source) {
        sourceBox->setCurrentIndex(i);
        break;
      }
    }
    if (i == sourceBox->count()) {
      std::cerr << "Cannot find source " << source.toStdString() << std::endl;
    }
  }
  updateSourceWarnings();

  enabledBox->setCheckState(rcEntry->enabled ? Qt::Checked : Qt::Unchecked);
  debugBox->setCheckState(rcEntry->debug ? Qt::Checked : Qt::Unchecked);
  automaticBox->setCheckState(rcEntry->automatic ? Qt::Checked : Qt::Unchecked);
  sitesEdit->setText(QString::fromStdString(rcEntry->onSite));
  reportEdit->setText(QString::number(rcEntry->reportPeriod));

  // Also save the parameter values:
  for (auto param : rcEntry->params) {
    if (! param->val) continue;
    if (verbose)
      std::cout << "Save value for param " << param->name << std::endl;
    setParamValues[param->name] = param->val;
  }
  resetParams();
}

conf::RCEntry *RCEntryEditor::getValue() const
{
  bool ok;
  double reportPeriod = reportEdit->text().toDouble(&ok);
  if (! ok) {
    std::cerr << "Cannot convert report period '"
              << reportEdit->text().toStdString()
              << "' into a double, using default" << std::endl;
    /* Use some default then. Would be nice if it were the same as
     * in RamenConsts. */
    reportPeriod = 30.;
  }
  conf::RCEntry *rce = new conf::RCEntry(
    nameEdit->text().toStdString(),
    enabledBox->checkState() == Qt::Checked,
    debugBox->checkState() == Qt::Checked,
    reportPeriod,
    sourceBox->currentText().toStdString(),
    sitesEdit->text().toStdString(),
    automaticBox->checkState() == Qt::Checked);

  // Add parameters (skipping those without a value):
  for (int row = 0; row < paramsForm->rowCount(); row ++) {
    QLayoutItem *item = paramsForm->itemAt(row, QFormLayout::LabelRole);
    QLabel *label = dynamic_cast<QLabel *>(item->widget());
    assert(label);
    std::string const pname(label->text().toStdString());
    item = paramsForm->itemAt(row, QFormLayout::FieldRole);
    AtomicWidget *editor = dynamic_cast<AtomicWidget *>(item->widget());
    assert(editor);
    std::shared_ptr<conf::Value const> val = editor->getValue();
    if (! val) continue;
    std::shared_ptr<conf::RamenValueValue const> rval =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(val);
    assert(rval);
    conf::RCEntryParam *param = new conf::RCEntryParam(pname, rval->v);
    rce->addParam(param);
  }

  return rce;
}

bool RCEntryEditor::isValid() const
{
  if (! sourceDoesExist || ! sourceIsCompiled) return false;
  if (! nameEdit->hasAcceptableInput()) return false;
  if (! sitesEdit->hasAcceptableInput()) return false;
  if (! reportEdit->hasAcceptableInput()) return false;

  return true;
}
