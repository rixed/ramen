Feature: Notifications work according to the configuration.

  We can use `ramen notify` to send generic notifications to the
  notifier, which must then behave according to its command line
  configuration.

  Background:
    Given ramen must be in the path.

  Scenario: Inexistant config file must fail.
    When I run ramen with argument notifier -c enoent.config
    Then ramen must fail gracefully.

  Scenario: Bad config file must fail.
    Given a file borken.config with content
      """
      pas glop
      """
    When I run ramen with argument notifier -c borken.config
    Then ramen must fail gracefully.

  Scenario: Canonicalworking exemple.
    Given a file sqlite.config with content
      """
      { teams = [
          { name = "" ;
            deferrable_contact = ViaSysLog "${name}: ${text} (${severity})" ;
            urgent_contact = ViaSqlite {
              file = "alerts.db" ;
              create = "create table \"alerts\" (
                  \"name\" text not null,
                  \"text\" text not null
                );" ;
              insert = "insert into \"alerts\" (
                  \"name\", \"text\"
                ) values (${name}, ${text});" } } ]
      }
      """
    And ramen notifier -c sqlite.config is started
    When I run ramen with argument notify test -p text=ouch
    Then ramen must exit gracefully
    And the query below against alerts.db must return ouch
      """
      SELECT "text" FROM "alerts" WHERE name="test"
      """
