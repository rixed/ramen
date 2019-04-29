#ifndef WIN_H_20190429
#define WIN_H_20190429

#include <QWidget>

class QPushButton;

class RmAdminWin : public QWidget
{
  Q_OBJECT
  QPushButton* button_;

public:
  explicit RmAdminWin(QWidget *parent = nullptr);
  ~RmAdminWin();

signals:

public slots:
//  void startup_progress();
};

#endif
