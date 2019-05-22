#ifndef STORAGETIMELINE_H_190522
#define STORAGETIMELINE_H_190522
#include <QGraphicsView>

class StorageTimeline : public QGraphicsView
{
  Q_OBJECT

public:
  StorageTimeline(QWidget *parent = nullptr);
};

#endif
