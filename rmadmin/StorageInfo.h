#ifndef STORAGEINFO_H_100522
#define STORAGEINFO_H_100522
#include <QSplitter>

class GraphModel;

class StorageInfo : public QSplitter
{
  Q_OBJECT

public:
  StorageInfo(GraphModel *, QWidget *parent = nullptr);
};

#endif
