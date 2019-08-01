#include <iostream>
#include <cassert>
#include <QVBoxLayout>
#include <QGridLayout>
#include <QLabel>
#include <QGroupBox>
#include "once.h"
#include "SourcesModel.h"
#include "RCEntryEditor.h"
#include "CodeInfoPanel.h"

CodeInfoPanel::CodeInfoPanel(QString const &sourceName, QWidget *parent) :
  AtomicWidget(conf::Key("sources/" + sourceName.toStdString() + "/info"), parent)
{
  widget = new QWidget;
  setCentralWidget(widget);

  QVBoxLayout *layout = new QVBoxLayout;
  widget->setLayout(layout);

  /* First: the SourceInfo (read only) */
  {
    infoLayout = new QGridLayout; // Why not a FormLayout?
    QWidget *infoBox = new QWidget;
    infoBox->setLayout(infoLayout);

    infoLayout->addWidget(new QLabel("Name:"), 0, 0, Qt::AlignRight);
    QLabel *nameLabel = new QLabel(sourceName);
    infoLayout->addWidget(nameLabel, 0, 1, Qt::AlignLeft);

    infoLayout->addWidget(new QLabel("MD5:"), 1, 0, Qt::AlignRight);
    md5Label = new QLabel;
    infoLayout->addWidget(md5Label, 1, 1, Qt::AlignLeft);

    // Either an error or normal info (notErr):
    errLabel = new QLabel;
    infoLayout->addWidget(errLabel, 2, 0, 1, 2, Qt::AlignHCenter);

    // Params (RO): Another grid inside the grid:
    paramBox = nullptr;
    // Function infos:
    functionBox = nullptr;

    layout->addWidget(infoBox);
  }

  /* Then the "run" area, to start a new program (ask for name, sites, etc).
   * Use a RC widget with a configurable name. */
  runBox = new RCEntryEditor(sourceName, false);
  layout->addWidget(runBox);

  // Connect the kvs value to setValue (read-only)
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  Once::connect(&kv, &KValue::valueCreated, this, &CodeInfoPanel::setValue);
  connect(&kv, &KValue::valueChanged, this, &CodeInfoPanel::setValue);
  // TODO: valueDeleted.
}

// If not visible then the error message will be visible
void CodeInfoPanel::setInfoVisible(bool visible)
{
  errLabel->setVisible(! visible);
  if (paramBox) paramBox->setVisible(visible);
  runBox->setVisible(visible);
  if (functionBox) functionBox->setVisible(visible);
}

bool CodeInfoPanel::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(v);
  if (! info) {
    std::cout << "source info not a SourceInfo?!" << std::endl;
    return false;
  }

  md5Label->setText(info->md5);
  if (info->isError()) {
    setInfoVisible(false);
    errLabel->setText(info->errMsg);
  } else {
    setInfoVisible(true);

    // TODO: a simple table would be nicer
    delete paramBox;
    paramBox = nullptr;
    if (! info->params.isEmpty()) {
      paramBox = new QGroupBox(tr("Parameters"));
      QGridLayout *layout = new QGridLayout;
      paramBox->setLayout(layout);
      int row = 0, column = 0;
      for (auto const param : info->params) {
        QLabel *pname = new QLabel(QString::fromStdString(param->name) + ":");
        layout->addWidget(pname, row, column, Qt::AlignRight);
        QLabel *pvalue = new QLabel(param->val->toQString(conf::Key::null));
        layout->addWidget(pvalue, row, column+1, Qt::AlignLeft);
        if (column >= 2) {
          column = 0;
          row ++;
        } else {
          column += 2;
        }
      }
      infoLayout->addWidget(paramBox, 4, 0, 1, 2, Qt::AlignHCenter);
    } else {
      infoLayout->addWidget(new QLabel("no parameters"), 4, 0, 1, 2, Qt::AlignHCenter);
    }

    delete functionBox;
    functionBox = nullptr;
    if (! info->infos.isEmpty()) {
      functionBox = new QGroupBox(tr("Functions"));
      QGridLayout *layout = new QGridLayout;
      functionBox->setLayout(layout);
      int row = 0;
      for (auto const func : info->infos) {
        layout->addWidget(new QLabel(func->name), row, 0, 1, 2, Qt::AlignHCenter);
        layout->addWidget(new QLabel(func->doc), row++, 0, 1, 2, Qt::AlignHCenter);
        layout->addWidget(new QLabel("Lazy?:"), row, 0, Qt::AlignRight);
        layout->addWidget(new QLabel(func->is_lazy ? tr("true"):tr("false")), row++, 1, Qt::AlignLeft);
        layout->addWidget(new QLabel("Sign.:"), row, 0, Qt::AlignRight);
        layout->addWidget(new QLabel(func->signature), row++, 1, Qt::AlignLeft);
        if (func->retention) {
          layout->addWidget(new QLabel("Retention:"), row++, 0, 1, 2, Qt::AlignHCenter);
          layout->addWidget(new QLabel("Duration:"), row, 0, Qt::AlignRight);
          layout->addWidget(new QLabel(QString::number(func->retention->duration)), row++, 1, Qt::AlignLeft);
          layout->addWidget(new QLabel("Period:"), row, 0, Qt::AlignRight);
          layout->addWidget(new QLabel(QString::number(func->retention->period)), row++, 1, Qt::AlignLeft);
        }
      }
      infoLayout->addWidget(functionBox, 5, 0, 1, 2, Qt::AlignHCenter);
    } else {
      infoLayout->addWidget(new QLabel("no functions!?"), 5, 0, 1, 2, Qt::AlignHCenter);
    }
  }
  emit valueChanged(k, v);

  return true;
}
