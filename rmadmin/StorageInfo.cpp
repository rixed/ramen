#include <QLabel>
#include <QSizePolicy>
#include "StorageForm.h"
#include "StorageInfoBox.h"
#include "StorageInfo.h"

StorageInfo::StorageInfo(GraphModel *graphModel, QWidget *parent) :
  QSplitter(parent)
{
  StorageForm *form = new StorageForm;
  addWidget(form);

  StorageInfoBox *infos = new StorageInfoBox(graphModel);
  addWidget(infos);

  // Make the edit form as narrow as possible:
  setStretchFactor(0, 0);
  setStretchFactor(1, 1);
}
