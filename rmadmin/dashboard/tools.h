#ifndef DASHBOARD_TOOLS_H_200304
#define DASHBOARD_TOOLS_H_200304
#include <functional>
#include <string>
#include <utility>
#include <QString>
#include "KValue.h"

std::pair<QString const, std::string const> dashboardNameAndPrefOfKey(
  std::string const &);

QString const dashboardNameOfKey(std::string const &);

std::string const dashboardPrefixOfKey(std::string const &);

void iterDashboards(
  std::function<void(std::string const &key, KValue const &,
                     QString const &, std::string const &key_prefix)>);

std::optional<int> widgetIndexOfKey(std::string const &);

void iterDashboardWidgets(
  std::string const &key_prefix,  // up to but not including "/widgets"
  std::function<void(std::string const &, KValue const &, int)>);

/* Returns the number of widgets defined in the dashboard which key prefix is
 * given. Prefix ends with the name of the dashboard (without trailing slash) */
int dashboardNumWidgets(std::string const &key_prefix);

/* Returns the next available widget number in a dashboard: */
int dashboardNextWidget(std::string const &key_prefix);

#endif
