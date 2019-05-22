#include <QLabel>
#include <QSizePolicy>
#include "StorageForm.h"
#include "StorageAllocs.h"
#include "StorageInfoBox.h"
#include "StorageInfo.h"

StorageInfo::StorageInfo(GraphModel *graphModel, QWidget *parent) :
  QSplitter(parent)
{
  StorageForm *form = new StorageForm;
  addWidget(form);

  StorageInfoBox *infos = new StorageInfoBox(graphModel);
  addWidget(infos);

  StorageAllocs *allocs = new StorageAllocs;
  addWidget(allocs);

  // Make the edit form as narrow as possible:
  setStretchFactor(0, 0);
  setStretchFactor(1, 1);
  setStretchFactor(2, 0);
}
