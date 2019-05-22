#ifndef STORAGEINFOBOX_H_190522
#define STORAGEINFOBOX_H_190522
#include <optional>
#include <QLabel>
#include <QTimer>

class FunctionItem;
class GraphModel;

class StorageInfoBox : public QWidget
{
  Q_OBJECT

  GraphModel *graphModel;
  QTimer recomputeTimer;

  /*
   * Infos to be displayed:
   */

  std::optional<unsigned> numArcWorkers;
  bool knowAllArcWorkers;
  QLabel *numArcWorkersWdg;

  std::optional<unsigned> numArcFiles;
  bool knowAllArcFiles;
  QLabel *numArcFilesWdg;

  std::optional<size_t> numArcBytes;
  bool knowAllArcBytes;
  QLabel *numArcBytesWdg;

  std::optional<float> lastAllocatorRun;
  QLabel *lastAllocatorRunWdg;

public:
  StorageInfoBox(GraphModel *, QWidget *parent = nullptr);

private slots:
  void rearmRecomputeTimer(FunctionItem const *);
  void recomputeStats();
};

#endif
