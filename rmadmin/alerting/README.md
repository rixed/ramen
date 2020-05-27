All objects related to alerting
===============================

Non-members of "alerting/"
--------------------------

Members of "alerting/"
----------------------

- AlertingStats: Displays global stats about all incidents etc. at the top
  of the AlertingWin

- AlertingTimeLine: A widget displaying a timeline of every incident, past and
  present in the selected time range (actually displays the journal, one
  incident per line).

- NotifTimeLine: An AbstractTimeline displaying log events of a given notification

- AlertingJournal: A widget displaying all logs of every incidents.

- AlertingLogsModel: A QAbstractTableModel for the AlertingJournal

- AlertingWin: The alerting window with all the above widgets.

