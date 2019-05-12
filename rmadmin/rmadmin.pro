######################################################################
# Automatically generated by qmake (3.1) Mon Apr 29 08:23:33 2019
######################################################################

CONFIG += c++17
CONFIG += debug

TEMPLATE = app
TARGET = rmadmin
VERSION = 3.4.0

INCLUDEPATH += .
INCLUDEPATH += /usr/local/include
INCLUDEPATH += /Users/rixed/.opam/ramen.4.07.1.flambda/lib/ocaml

QMAKE_MACOSX_DEPLOYMENT_TARGET = 10.14
QMAKE_CXXFLAGS = -Wall -Wextra -fstandalone-debug
QT += core widgets

# The following define makes your compiler warn you if you use any
# feature of Qt which has been marked as deprecated (the exact warnings
# depend on your compiler). Please consult the documentation of the
# deprecated API in order to know how to port your code away from it.
DEFINES += QT_DEPRECATED_WARNINGS

# You can also make your code fail to compile if you use deprecated APIs.
# In order to do so, uncomment the following line.
# You can also select to disable deprecated APIs only up to a certain version of Qt.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

# Input
HEADERS += \
  LazyRef.h \
  colorOfString.h \
  SyncStatus.h \
  PosIntValidator.h \
  PosDoubleValidator.h \
  confKey.h \
  confValue.h \
  KValue.h \
  conf.h \
  KLineEdit.h \
  KLabel.h \
  KErrorMsg.h \
  AtomicWidget.h \
  AtomicForm.h \
  GraphAnchor.h \
  OperationsItem.h \
  FunctionItem.h \
  ProgramItem.h \
  SiteItem.h \
  GraphViewSettings.h \
  OperationsModel.h \
  OperationsView.h \
  GraphArrow.h \
  layoutNode.h \
  layout.h \
  GraphView.h \
  StorageForm.h \
  RmAdminWin.h

SOURCES += \
  colorOfString.cpp \
  SyncStatus.cpp \
  PosIntValidator.cpp \
  PosDoubleValidator.cpp \
  confKey.cpp \
  confValue.cpp \
  KValue.cpp \
  conf.cpp \
  AtomicForm.cpp \
  GraphAnchor.cpp \
  OperationsItem.cpp \
  FunctionItem.cpp \
  ProgramItem.cpp \
  SiteItem.cpp \
  GraphViewSettings.cpp \
  OperationsModel.cpp \
  OperationsView.cpp \
  GraphArrow.cpp \
  layoutNode.cpp \
  layout.cpp \
  GraphView.cpp \
  StorageForm.cpp \
  RmAdminWin.cpp \
  main.cpp

LIBS += GuiHelper.o
LIBS += -L/Users/rixed/.opam/ramen.4.07.1.flambda/lib/ocaml -lunix -lbigarray -lnums -lcamlstr -lthreads -lasmrun_shared
LIBS += -L/Users/rixed/.opam/ramen.4.07.1.flambda/lib/stdint -lstdint_stubs
LIBS += -L/Users/rixed/.opam/ramen.4.07.1.flambda/lib/zmq -lzmq_stubs
LIBS += -L/usr/local/lib -lzmq
LIBS += -L/usr/local/lib -lz3
