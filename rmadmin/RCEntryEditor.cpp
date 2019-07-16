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
#include "SourcesModel.h"  // for sourceNameOfKey and friends
#include "RamenValueEditor.h"
#include "confRCEntryParam.h"
#include "RCEntryEditor.h"

static bool const debug = false;

/* static bool isCompiledFile(QMap<conf::Key, KValue>::const_iterator const &kvIt)
{
  std::string const &key = kvIt.key().s;
  if (! startsWith(key, "sources/") || ! endsWith(key, "/info")) return false;

  KValue const &kv = kvIt.value();
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.value());
  if (! info) return false;

  return info->isInfo();
} */

static bool isSourceFile(std::string const &key)
{
  return startsWith(key, "sources/") && ! endsWith(key, "/info");
}

RCEntryEditor::RCEntryEditor(QString const &sourceName, bool sourceEditable_, QWidget *parent) :
  QWidget(parent),
  sourceEditable(sourceEditable_)
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  nameEdit = new QLineEdit;
  layout->addRow(tr("Program &Name:"), nameEdit);

  { // source

    QVBoxLayout *sourceLayout = new QVBoxLayout;
    sourceBox = new QComboBox;
    /* We may receive the source only later.
     * We do not want to select anything else though. So, create the combo
     * entry for the expected source right now and make sure resetSources
     * can deal with this situation: */
    sourceBox->addItem(sourceName);
    sourceBox->setEnabled(sourceEditable);

    sourceLayout->addWidget(sourceBox);

//    connect(sourceBox, &QComboBox::currentIndexChanged,
//            this, &RCEntryEditor::resetParams);
    connect(sourceBox, QOverload<int>::of(&QComboBox::currentIndexChanged),
            [=](int){ resetParams(); });

    deletedSourceWarning =
      new QLabel(tr("This source does not exist any longer!"));
    deletedSourceWarning->setVisible(false);
    sourceLayout->addWidget(deletedSourceWarning);

    layout->addRow(tr("Source:"), sourceLayout);
  }

  { // flags
    QVBoxLayout *flagsLayout = new QVBoxLayout;
    enabledBox = new QCheckBox("enabled");
    flagsLayout->addWidget(enabledBox);
    debugBox = new QCheckBox("debug mode");
    flagsLayout->addWidget(debugBox);
    automaticBox = new QCheckBox("automatic");
    automaticBox->setEnabled(false);
    flagsLayout->addWidget(automaticBox);
    layout->addRow(tr("Flags:"), flagsLayout);
  }

  sitesEdit = new QLineEdit;
  layout->addRow(tr("&Only on Sites:"), sitesEdit);

  reportEdit = new QLineEdit;
  layout->addRow(tr("&Reporting Period:"), reportEdit);

  paramsForm = new QFormLayout;
  layout->addRow(new QLabel(tr("Parameters:")));
  layout->addRow(paramsForm);

  conf::autoconnect("^sources/.*", [this](conf::Key const &k, KValue const *) {
    if (isSourceFile(k.s)) resetSources();
  });
}

RCEntryEditor::RCEntryEditor(conf::RCEntry const *rcEntry, QWidget *parent) :
  RCEntryEditor(QString::fromStdString(rcEntry->source), true, parent)
{
  setValue(rcEntry);
}

void RCEntryEditor::resetSources()
{
  /* Iter over both the list of existing sources and the combo and update the
   * combo. Do not delete the selected entry but make the deletedSourceWarning
   * visible instead (otherwise, make it invisible).
   * If the combobox was empty, select the first item and call resetParams.
   */
  conf::kvs_lock.lock_shared();
  QMap<conf::Key, KValue>::const_iterator kvIt(conf::kvs.constBegin());
  int comboIdx = 0;

  sourceDoesExist = true;
  while (kvIt != conf::kvs.constEnd() || comboIdx < sourceBox->count()) {
    /* "Left" being the kvs enumeration and "right" being the combo entries,
     * at each step those actions are possible:
     * - advance left, whenever it's not a source
     * - advance left and right, if they are equal
     * - add left entry before right entry, and advance left, if left is
     *   before right
     * - remove right entry (which advance right), otherwise (unless it's
     *   selected, see above)
     */
    bool removeRight = false;
    if (kvIt == conf::kvs.constEnd()) {
      if (debug) std::cout << "At end of keys" << std::endl;
      removeRight = true;
    } else {
      if (! isSourceFile(kvIt.key().s)) {
        kvIt ++;
      } else { // left is a source file (or exhausted)
        QString const left = sourceNameOfKey(kvIt.key());
        if (comboIdx >= sourceBox->count()) {
          // insert at the end before there is nothing left to compare with
          if (debug) std::cout << "Append " << left.toStdString() << " to combo" << std::endl;
          sourceBox->addItem(left);
          comboIdx ++;
          kvIt ++;
        } else {
          // there is a left and a right, compare them:
          QString const right = sourceBox->itemText(comboIdx);
          int const cmp = QString::compare(left, right);
          if (cmp == 0) {
            if (debug) std::cout << "Same key: " << left.toStdString() << std::endl;
            kvIt ++;
            comboIdx ++;
          } else if (cmp < 0) {
            if (debug) std::cout << "Insert key " << left.toStdString() << " at " << comboIdx << std::endl;
            sourceBox->insertItem(comboIdx, left);
            comboIdx ++;
            kvIt ++;
          } else {
            if (debug) std::cout << "Removing key from combo" << std::endl;
            removeRight = true;
          }
        }
      }
    }
    if (removeRight && comboIdx < sourceBox->count()) {
      if (comboIdx == sourceBox->currentIndex()) {
        std::cout << "Selected combo entry (" << sourceBox->currentText().toStdString() << ") does not exist" << std::endl;
        sourceDoesExist = false;
        comboIdx ++;
      } else {
        if (debug) std::cout << "Removing combo entry " << sourceBox->itemText(comboIdx).toStdString() << std::endl;
        sourceBox->removeItem(comboIdx);
      }
    }
  }
  conf::kvs_lock.unlock_shared();

  deletedSourceWarning->setVisible(!sourceDoesExist);
}

void RCEntryEditor::setSourceExists(bool s)
{
  sourceDoesExist = s;
  deletedSourceWarning->setVisible(!s);
  // TODO: also disable the rest of the form
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
  std::cout << "paramValue(" << p->name << ") is "
            << (setParamValues.contains(p->name) ? "present" : "absent") << std::endl;
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
    std::string const &pname = label->text().toStdString();
    item = paramsForm->itemAt(row, QFormLayout::FieldRole);
    RamenValueEditor *editor = dynamic_cast<RamenValueEditor *>(item->widget());
    assert(editor);
    setParamValues[pname] = std::shared_ptr<RamenValue const>(editor->getValue());
  }

  QString const baseName = removeExtQ(sourceBox->currentText());
  conf::Key infoKey("sources/" + baseName.toStdString() + "/info");

  conf::kvs_lock.lock_shared();
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(conf::kvs[infoKey].val);
  conf::kvs_lock.unlock_shared();

  clearParams();

  if (! info) {
    std::cout << "Cannot get info for " << baseName.toStdString() << std::endl;
    setSourceExists(false);
    return;
  }

  for (auto const p : info->params) {
    /* TODO: instead of a generic QLineEdit, use p->value->editor(), given p
     * is a CompiledProgramParam and p->value a RamenValue, which should
     * be able to create its own editor. */
    // TODO: a tooltip with the parameter doc (CompiledProgramParam doc)
    std::shared_ptr<RamenValue const> val = paramValue(p);
    RamenValueEditor *paramEdit = RamenValueEditor::ofType(p->val->structure, val.get());
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
      std::cout << "Cannot find source " << source.toStdString() << std::endl;
    }
    setSourceExists(i < sourceBox->count());
  }

  enabledBox->setCheckState(rcEntry->enabled ? Qt::Checked : Qt::Unchecked);
  debugBox->setCheckState(rcEntry->debug ? Qt::Checked : Qt::Unchecked);
  automaticBox->setCheckState(rcEntry->automatic ? Qt::Checked : Qt::Unchecked);
  sitesEdit->setText(QString::fromStdString(rcEntry->onSite));
  reportEdit->setText(QString::number(rcEntry->reportPeriod));

  // Also save the parameter values:
  for (auto param : rcEntry->params) {
    std::cout << "Save value for param " << param->name << std::endl;
    setParamValues[param->name] = std::shared_ptr<RamenValue const>(param->val);
  }
  resetParams();
}

conf::RCEntry *RCEntryEditor::getValue() const
{
  bool ok;
  double reportPeriod = reportEdit->text().toDouble(&ok);
  if (! ok) {
    std::cout << "Cannot convert report period " << reportEdit->text().toStdString()
              << " into a double" << std::endl;
    // so be it
  }
  conf::RCEntry *rce = new conf::RCEntry(
    nameEdit->text().toStdString(),
    enabledBox->checkState() == Qt::Checked,
    debugBox->checkState() == Qt::Checked,
    reportPeriod,
    sourceBox->currentText().toStdString(),
    sitesEdit->text().toStdString(),
    automaticBox->checkState() == Qt::Checked);

  // Add parameters:


  return rce;
}
