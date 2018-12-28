Feature: Notifications work according to the configuration.

  We can use `ramen notify` to send generic notifications to the
  alerter, which must then behave according to its command line
  configuration.

  Background:
    Given ramen must be in the path.

  Scenario: Nonexistent config file must fail.
    When I run ramen with argument alerter -c enoent.config
    Then ramen must fail gracefully.

  Scenario: Bad config file must fail.
    Given a file borken.config with content
      """
      pas glop
      """
    When I run ramen with argument alerter -c borken.config
    Then ramen must fail gracefully.

  Scenario: Canonical working example.
    Given a file sqlite.config with content
      """
      { teams = [
          { contacts = [
              ViaSqlite {
                file = "alerts.db" ;
                create = "create table \"alerts\" (
                    \"alert_id\" integer not null,
                    \"name\" text not null,
                    \"text\" text not null
                  );" ;
                insert = "insert into \"alerts\" (
                    \"alert_id\", \"name\", \"text\"
                  ) values (${alert_id}, ${name}, ${text});" } ] } ] ;
        default_init_schedule_delay = 0 ;
        default_init_schedule_delay_after_startup = 0 }
      """
    And ramen alerter -c sqlite.config is started
    When I run ramen with argument notify test -p text=ouch
    Then ramen must exit gracefully
    And the query below against alerts.db must return 1,ouch
      """
      SELECT "alert_id", "text" FROM "alerts" WHERE name="test"
      """

  Scenario: It's possible to tail from notifications
    When I run ramen with argument notify test -p param1=foobar
    And I run ramen with argument tail '#notifs'
    Then after max 1 second ramen must exit gracefully
    And ramen must mention "foobar" on stdout.
