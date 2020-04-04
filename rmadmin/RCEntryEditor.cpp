#include <cassert>
#include <string>
#include <memory>
#include <QtGlobal>
#include <QDebug>
#include <QFormLayout>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QCheckBox>
#include <QComboBox>
#include <QLabel>
#include "conf.h"
#include "misc.h"
#include "SourcesModel.h"  // for baseNameOfKey and friends
#include "RangeDoubleValidator.h"
#include "PathSuffixValidator.h"
#include "confRCEntryParam.h"
#include "AtomicWidget.h"
#include "RCEntryEditor.h"

static bool const verbose(false);

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
  return ! endsWith(key, "/info");
}

static bool isInfoFile(std::string const &key)
{
  return endsWith(key, "/info");
}

RCEntryEditor::RCEntryEditor(bool sourceEditable_, QWidget *parent) :
  QWidget(parent),
  sourceDoesExist(false),
  sourceIsCompiled(false),
  enabled(false),
  sourceEditable(sourceEditable_)
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  { // source

    QVBoxLayout *sourceLayout = new QVBoxLayout;
    sourceBox = new QComboBox;
    sourceBox->setEnabled(sourceEditable);

    sourceLayout->addWidget(sourceBox);

    connect(sourceBox, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, [this](int) {
      updateSourceWarnings();
      saveParams();
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

  { // suffix
    suffixEdit = new QLineEdit;
    suffixEdit->setPlaceholderText(tr("Program Suffix"));
    suffixEdit->setValidator(new PathSuffixValidator(this));
    layout->addRow(tr("Program &Suffix:"), suffixEdit);
    connect(suffixEdit, &QLineEdit::textChanged,
            this, &RCEntryEditor::inputChanged);
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

  cwdEdit = new QLineEdit;
  connect(cwdEdit, &QLineEdit::textChanged,
          this, &RCEntryEditor::inputChanged);
  layout->addRow(tr("Working &Directory:"), cwdEdit);

  paramsForm = new QFormLayout;
  layout->addRow(new QLabel(tr("Parameters:")), paramsForm);

  setEnabled(enabled);

  /* Each creation/deletion of source files while this editor is alive should
   * refresh the source select box and its associated warnings: */
  connect(kvs, &KVStore::keyChanged,
          this, &RCEntryEditor::onChange);
}

void RCEntryEditor::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        addSourceFromStore(change.key, change.kv);
        break;
      case KeyChanged:
        updateSourceFromStore(change.key, change.kv);
        break;
      case KeyDeleted:
        removeSourceFromStore(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void RCEntryEditor::addSourceFromStore(std::string const &key, KValue const &)
{
  if (! startsWith(key, "sources/")) return;

  if (verbose)
    qDebug() << "RCEntryEditor::addSourceFromStore: New key:"
             << QString::fromStdString(key);

  if (isSourceFile(key)) {
    if (verbose)
      qDebug() << "RCEntryEditor::addSourceFromStore: ... is a source key";
    addSource(key); // Won't change the selection but might change warnings
    updateSourceWarnings();
  } else if (isInfoFile(key)) {
    if (verbose)
      qDebug() << "RCEntryEditor::addSourceFromStore: ... is an info key";
    updateSourceWarnings();
  }
}

void RCEntryEditor::updateSourceFromStore(std::string const &key, KValue const &)
{
  if (! startsWith(key, "sources/")) return;

  if (verbose)
    qDebug() << "RCEntryEditor::updateSourceFromStore: Upd key:"
             << QString::fromStdString(key);

  if (isInfoFile(key)) {
    if (verbose)
      qDebug() << "RCEntryEditor::updateSourceFromStore: ... is an info key";
    updateSourceWarnings();
  }
}

void RCEntryEditor::removeSourceFromStore(std::string const &key, KValue const &)
{
  if (! startsWith(key, "sources/")) return;

  /* Do not remove anything, so keep the current selection.
   * Just update the warning: */
  updateSourceWarnings();
}

void RCEntryEditor::setProgramName(std::string const &programName)
{
  /* Even if this sourceName does not exist we want to have it preselected
   * (with a warning displayed).
   * If several non-existing sourceNames are set, they will accumulate in
   * the box. Not a big deal as long as the warnings are properly displayed. */
  std::string const srcPath = srcPathFromProgramName(programName);
  std::string const programSuffix = suffixFromProgramName(programName);
  suffixEdit->setText(QString::fromStdString(programSuffix));

  int index = findOrAddSourceName(QString::fromStdString(srcPath));
  if (index >= 0) sourceBox->setCurrentIndex(index);
}

void RCEntryEditor::setEnabled(bool enabled_)
{
  if (verbose)
    qDebug() << "RCEntryEditor::setEnabled(" << enabled << ")";

  enabled = enabled_; // Save it for resetParams

  suffixEdit->setEnabled(enabled);
  sourceBox->setEnabled(sourceEditable && enabled);
  enabledBox->setEnabled(enabled);
  debugBox->setEnabled(enabled);
  sitesEdit->setEnabled(enabled);
  reportEdit->setEnabled(enabled);
  cwdEdit->setEnabled(enabled);
  for (int row = 0; row < paramsForm->rowCount(); row ++) {
    QLayoutItem *item = paramsForm->itemAt(row, QFormLayout::LabelRole);
    item = paramsForm->itemAt(row, QFormLayout::FieldRole);
    AtomicWidget *editor = dynamic_cast<AtomicWidget *>(item->widget());
    assert(editor);
    editor->setEnabled(enabled);
  }
}

int RCEntryEditor::addSource(std::string const &k)
{
  return findOrAddSourceName(baseNameOfKey(k));
}

int RCEntryEditor::findOrAddSourceName(QString const &name)
{
  if (name.isEmpty()) return -1;

  // Insert in alphabetic order:
  for (int i = 0; i <= sourceBox->count(); i ++) {
    // Do not add it once more (can happen when it was preselected)
    if (i < sourceBox->count() &&
        sourceBox->itemText(i) == name) {
      return i;
    }
    if (i == sourceBox->count() ||
        name < sourceBox->itemText(i)) {
      /* Add this to the combo but leave the selected index unchanged.
       * This avoids costly calls to the lambda currentIndexChanged is
       * connected to. */
      int const idx = sourceBox->currentIndex();
      sourceBox->insertItem(i, name);
      sourceBox->setCurrentIndex(idx);
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
    std::string info_k(keyOfSourceName(name, "info"));
    kvs->lock.lock_shared();
    sourceDoesExist =
      kvs->map.find(keyOfSourceName(name, "ramen")) != kvs->map.end() ||
      kvs->map.find(keyOfSourceName(name, "alert")) != kvs->map.end();
    auto it = kvs->map.find(info_k);
    sourceIsCompiled =
      it != kvs->map.end() &&
      isCompiledSource(it->second);
    kvs->lock.unlock_shared();
  }

  deletedSourceWarning->setVisible(!sourceDoesExist);
  notCompiledSourceWarning->setVisible(!sourceIsCompiled);
}

void RCEntryEditor::clearParams()
{
  // Note: emptyLayout(paramsForm) won't update the QFormLayout rowCount :(
  while (paramsForm->rowCount() > 0)
    paramsForm->removeRow(0); // Note: this also deletes the widgets
}

std::shared_ptr<RamenValue const> RCEntryEditor::paramValue(
  std::shared_ptr<CompiledProgramParam const> p) const
{
  /* Try to find a set parameter by that name, falling back on the
   * compiled default: */
  if (verbose)
    qDebug() << "RCEntryEditor: paramValue("
             << QString::fromStdString(p->name) << ") is"
             << (setParamValues.contains(p->name) ? "present" : "absent");
  return setParamValues.value(p->name, p->val);
}

/* Not the brightest idea to use labels as value store, but there you go: */
static QString const labelOfParamName(std::string const &pname)
{
  return QString::fromStdString(pname) + ":";
}
static std::string const paramNameOfLabel(QString const &label)
{
  assert(label.length() > 0);
  return removeAmp(label.left(label.length()-1)).toStdString();
}

/* Save the values that are currently set (and that can be parsed) into
 * setParamValues, for later reuse. */
void RCEntryEditor::saveParams()
{
  for (int row = 0; row < paramsForm->rowCount(); row ++) {
    QLayoutItem *item = paramsForm->itemAt(row, QFormLayout::LabelRole);
    QLabel *label = dynamic_cast<QLabel *>(item->widget());
    assert(label);
    std::string const pname(paramNameOfLabel(label->text()));

    item = paramsForm->itemAt(row, QFormLayout::FieldRole);
    AtomicWidget *editor = dynamic_cast<AtomicWidget *>(item->widget());
    assert(editor);
    std::shared_ptr<conf::RamenValueValue const> rval =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(editor->getValue());
    if (rval && rval->v) {
      if (verbose)
        qDebug() << "RCEntryEditor: set paramValues["
                 << QString::fromStdString(pname) << "] to" << *rval->v;
      setParamValues[pname] = rval->v;
    } else {
      qCritical() << "AtomicWidget editor returned a confValue for row" << row
                  << "(name" << QString::fromStdString(pname)
                  << ") that's not a RamenValueValue!?";
    }
  }
}

void RCEntryEditor::resetParams()
{
  /* Clear the paramsForm and rebuilt it, taking values from saved values */
  clearParams();

  QString const baseName = sourceBox->currentText();
  if (baseName.isEmpty()) return;

  std::string infoKey("sources/" + baseName.toStdString() + "/info");

  kvs->lock.lock_shared();
  std::shared_ptr<conf::SourceInfo const> info;
  auto it = kvs->map.find(infoKey);
  if (it != kvs->map.end())
    info = std::dynamic_pointer_cast<conf::SourceInfo const>(it->second.val);
  kvs->lock.unlock_shared();

  if (! info) {
    /* This can be normal during sync though: */
    if (! initial_sync_finished) {
      qDebug() << "RCEntryEditor: Cannot get info"
               << QString::fromStdString(infoKey)
               << ", more luck later when sync is complete.";
    } else {
      qWarning() << "Cannot get info" << QString::fromStdString(infoKey);
      qWarning() << "conf map is:";
      for (auto &it : kvs->map) {
        qWarning() << "  " << QString::fromStdString(it.first)
                   << "->" << it.second.val->toQString(it.first);
      }
    }
    return;
  }

  for (auto &p : info->params) {
    // TODO: a tooltip with the parameter doc (CompiledProgramParam doc)
    std::shared_ptr<RamenValue const> val = paramValue(p);
    AtomicWidget *paramEdit = val->editorWidget(std::string());
    /* In theory, AtomicWidget got their value from the key. But here we
     * have no key but we know the value so let's just set it: */
    std::shared_ptr<conf::RamenValueValue const> confval =
      std::make_shared<conf::RamenValueValue const>(val);

    paramEdit->setValue(
      std::string(), std::static_pointer_cast<conf::Value const>(confval));
    paramEdit->setEnabled(enabled);
    connect(paramEdit, &AtomicWidget::inputChanged,
            this, &RCEntryEditor::inputChanged);
    paramsForm->addRow(labelOfParamName(p->name), paramEdit);
  }
}

void RCEntryEditor::setValue(conf::RCEntry const &rcEntry)
{
  if (rcEntry.programName.length() == 0) {
    suffixEdit->setEnabled(false);
    sourceBox->setEnabled(false);
  } else {
    std::string const srcPath = srcPathFromProgramName(rcEntry.programName);
    std::string const programSuffix = suffixFromProgramName(rcEntry.programName);
    suffixEdit->setText(QString::fromStdString(programSuffix));

    suffixEdit->setEnabled(enabled);
    sourceBox->setEnabled(sourceEditable && enabled);
    QString const source = QString::fromStdString(srcPath);
    int const i = findOrAddSourceName(source);
    sourceBox->setCurrentIndex(i);
  }
  updateSourceWarnings();

  enabledBox->setCheckState(rcEntry.enabled ? Qt::Checked : Qt::Unchecked);
  debugBox->setCheckState(rcEntry.debug ? Qt::Checked : Qt::Unchecked);
  automaticBox->setCheckState(rcEntry.automatic ? Qt::Checked : Qt::Unchecked);
  sitesEdit->setText(QString::fromStdString(rcEntry.onSite));
  reportEdit->setText(QString::number(rcEntry.reportPeriod));
  cwdEdit->setText(QString::fromStdString(rcEntry.cwd));

  // Also save the parameter values so that resetParams can find them:
  for (auto const &param : rcEntry.params) {
    if (! param->val) continue;
    if (verbose)
      qDebug() << "RCEntryEditor: Save value" << *param->val
               << "for param" << QString::fromStdString(param->name);
    setParamValues[param->name] = param->val;
  }
  resetParams();
}

conf::RCEntry *RCEntryEditor::getValue() const
{
  bool ok;
  double reportPeriod = reportEdit->text().toDouble(&ok);
  if (! ok) {
    qCritical() << "Cannot convert report period"
                << reportEdit->text()
                << "into a double, using default";
    /* Use some default then. Would be nice if it were the same as
     * in RamenConsts. */
    reportPeriod = 30.;
  }

  std::string const cwd(cwdEdit->text().toStdString());

  std::string const programName(
    suffixEdit->text().isEmpty() ?
      sourceBox->currentText().toStdString() :
      sourceBox->currentText().toStdString() + '#' +
      suffixEdit->text().toStdString());

  conf::RCEntry *rce = new conf::RCEntry(
    programName,
    enabledBox->checkState() == Qt::Checked,
    debugBox->checkState() == Qt::Checked,
    reportPeriod,
    cwd,
    sitesEdit->text().toStdString(),
    automaticBox->checkState() == Qt::Checked);

  // Add parameters (skipping those without a value):
  for (int row = 0; row < paramsForm->rowCount(); row ++) {
    QLayoutItem *item = paramsForm->itemAt(row, QFormLayout::LabelRole);
    QLabel *label = dynamic_cast<QLabel *>(item->widget());
    assert(label);
    std::string const pname(paramNameOfLabel(label->text()));

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
  if (! suffixEdit->hasAcceptableInput()) return false;
  if (! sitesEdit->hasAcceptableInput()) return false;
  if (! reportEdit->hasAcceptableInput()) return false;

  return true;
}
