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
            deferrable_contacts =
              [ ViaSysLog "${name}: ${text} (${severity})" ] ;
            urgent_contacts = [
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
    And ramen notifier -c sqlite.config --debug is started
    When I run ramen with argument notify test -p text=ouch
    Then ramen must exit gracefully
    And the query below against alerts.db must return 1,ouch
      """
      SELECT "alert_id", "text" FROM "alerts" WHERE name="test"
      """
