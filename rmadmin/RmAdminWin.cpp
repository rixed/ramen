#include <QtWidgets>
#include "RmAdminWin.h"

RmAdminWin::RmAdminWin(QWidget *parent) :
    QWidget(parent)
{
   button_ = new QPushButton(tr("Push Me!"));

   QGridLayout *mainLayout = new QGridLayout;
   mainLayout->addWidget(button_,0,0);
   setLayout(mainLayout);
   setWindowTitle(tr("Connecting buttons to processes.."));
}

// Destructor
RmAdminWin::~RmAdminWin()
{
   delete button_;
}
