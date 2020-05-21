Feature: Notifications work according to the configuration.

  We can use `ramen notify` to send generic notifications to the
  alerter, which must then behave according to its command line
  configuration.

  Background:
    Given the whole gang is started

  Scenario: Canonical working example.
    Given configuration key alerting/teams/test/contacts/to-sql is set to:
      """
      { via = Sqlite {
                file = "alerts.db" ;
                create = "create table \"alerts\" (
                    \"incident_id\" integer not null,
                    \"name\" text not null,
                    \"text\" text not null
                  );" ;
                insert = "insert into \"alerts\" (
                    \"incident_id\", \"name\", \"text\"
                  ) values (${incident_id|sql}, ${name|sql}, ${text|sql});" } }
      """
    When I run ramen with argument notify test -p text=ouch -p debounce=0.1
    Then ramen must exit gracefully
    And the query below against alerts.db must return ouch
      """
      SELECT "text" FROM "alerts" WHERE name="test"
      """
