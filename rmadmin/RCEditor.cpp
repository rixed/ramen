#include <cassert>
#include <string>
#include <memory>
#include <QFormLayout>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QCheckBox>
#include <QComboBox>
#include <QLabel>
#include "conf.h"
#include "misc.h"
#include "SourcesModel.h"  // for sourceNameOfKey
#include "RCEditor.h"

RCEditor::RCEditor(QString const &sourceName_, QWidget *parent) :
  QWidget(parent),
  sourceName(sourceName_)
{
  QFormLayout *layout = new QFormLayout;
  setLayout(layout);

  QLineEdit *nameEdit = new QLineEdit;
  layout->addRow(tr("Program &Name:"), nameEdit);

  { // source
    QVBoxLayout *sourceLayout = new QVBoxLayout;
    sourceBox = new QComboBox;
    if (! sourceName.isEmpty())
      sourceBox->setEnabled(false);

    sourceLayout->addWidget(sourceBox);

    deletedSourceWarning =
      new QLabel(tr("This source does not exist any longer!"));
    deletedSourceWarning->setVisible(false);
    sourceLayout->addWidget(deletedSourceWarning);

    layout->addRow(tr("&Source:"), sourceLayout);
  }

  { // flags
    QVBoxLayout *flagsLayout = new QVBoxLayout;
    QCheckBox *enabledBox = new QCheckBox("enabled");
    flagsLayout->addWidget(enabledBox);
    QCheckBox *debugBox = new QCheckBox("debug mode");
    flagsLayout->addWidget(debugBox);
    QCheckBox *automaticBox = new QCheckBox("automatic");
    automaticBox->setEnabled(false);
    flagsLayout->addWidget(automaticBox);
    layout->addRow(tr("Flags:"), flagsLayout);
  }

  QLineEdit *sitesEdit = new QLineEdit;
  layout->addRow(tr("&Only on Sites:"), sitesEdit);

  QLineEdit *reportEdit = new QLineEdit;
  layout->addRow(tr("&Reporting Period:"), reportEdit);

  paramsForm = new QFormLayout;
  layout->addRow(new QLabel(tr("Parameters:")));
  layout->addRow(paramsForm);

  resetSources();
}

static bool isCompiledSource(QMap<conf::Key, KValue>::const_iterator const &kvIt)
{
  std::string const &key = kvIt.key().s;
  if (! startsWith(key, "sources/") || ! endsWith(key, "/info")) return false;

  KValue const &kv = kvIt.value();
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.value());
  if (! info) return false;

  return info->isInfo();
}

void RCEditor::resetSources()
{
  bool const comboWasEmpty = sourceBox->count() == 0;

  /* Iter over both the list of existing sources and the combo and update the
   * combo. Do not delete the selected entry but make the deletedSourceWarning
   * visible instead (otherwise, make it invisible).
   * If the combobox was empty, select the first item and call changedSource.
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
      std::cout << "At end of keys" << std::endl;
      removeRight = true;
    } else {
      if (! isCompiledSource(kvIt)) {
        kvIt ++;
      } else { // left is a compiled source (or exhausted)
        QString const left = sourceNameOfKey(kvIt.key());
        if (comboIdx >= sourceBox->count()) {
          // insert at the end before there is nothing left to compare with
          std::cout << "Append " << left.toStdString() << " to combo" << std::endl;
          sourceBox->addItem(left);
          if (left == sourceName) sourceBox->setCurrentIndex(comboIdx);
          comboIdx ++;
          kvIt ++;
        } else {
          // there is a left and a right, compare them:
          QString const right = sourceBox->itemText(comboIdx);
          int const cmp = QString::compare(left, right);
          if (cmp == 0) {
            std::cout << "Same key: " << left.toStdString() << std::endl;
            kvIt ++;
            comboIdx ++;
          } else if (cmp < 0) {
            std::cout << "Insert key " << left.toStdString() << " at " << comboIdx << std::endl;
            sourceBox->insertItem(comboIdx, left);
            if (left == sourceName) sourceBox->setCurrentIndex(comboIdx);
            comboIdx ++;
            kvIt ++;
          } else {
            std::cout << "Removing key from combo" << std::endl;
            removeRight = true;
          }
        }
      }
    }
    if (removeRight && comboIdx < sourceBox->count()) {
      if (comboIdx == sourceBox->currentIndex()) {
        std::cout << "Selected combo entry does not exist" << std::endl;
        sourceDoesExist = false;
        comboIdx ++;
      } else {
        sourceBox->removeItem(comboIdx);
      }
    }
  }

  deletedSourceWarning->setVisible(!sourceDoesExist);

  if (comboWasEmpty && sourceBox->count() > 0) changedSource();

  conf::kvs_lock.unlock_shared();
}

void RCEditor::setSourceExists(bool s)
{
  sourceDoesExist = s;
  deletedSourceWarning->setVisible(!s);
  // TODO: also disable the rest of the form
}

void RCEditor::clearParams()
{
  while (paramsForm->rowCount() > 0)
    paramsForm->removeRow(0);
}

void RCEditor::changedSource()
{
  /* Clear the paramsForm and rebuilt it.
   * Maybe save the values that are set in a global map of parameter_name to
   * value, to populate next param lists? Also do this when submitting that
   * form. */
  QString const sourceName = sourceBox->currentText();
  conf::Key infoKey("sources/" + sourceName.toStdString() + "/info");

  conf::kvs_lock.lock_shared();
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(conf::kvs[infoKey].value());
  conf::kvs_lock.unlock_shared();

  clearParams();

  if (! info) {
    std::cout << "Cannot get info for " << sourceName.toStdString() << std::endl;
    setSourceExists(false);
    return;
  }

  for (auto const p : info->params) {
    QLineEdit *paramEdit = new QLineEdit;
    paramsForm->addRow(p->name, paramEdit);
  }
}
