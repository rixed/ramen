Feature: Test Ramen API from the command line.

  Test basic interactions with the API over HTTP.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And a file test.ramen with content
      """
      define color_of_the_day as
        select now as t, random as v,
               u64(t) % 2 = 1 as odd,
               case when random > 0.66 then "blue"
                    when random > 0.50 then "red"
                    else "green"
               end as color
        every 1 second
        factor color
        event starts at t;

      define random_walk as
        from (select sum globally 1 as seq every 1 second)
        select now as t, coalesce(previous.xyz, 0) + (random*2-1) as xyz
        event starts at t with duration 1s;
      """
    And test.ramen is compiled
    And the program test is running.

  Scenario: Can get the version number from the API.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"version","id":"123"}' http://localhost:8042
    Then curl must mention ""result":"v3.0.12""
    And curl must mention ""id":"123"".

  Scenario: Api can be given its own path on top of server path.
    Given ramen httpd --url http://localhost:8042/ramen --api=api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"version","id":"123"}' http://localhost:8042/ramen/api
    Then curl must mention ""result":"v3.0.12""
    And curl must mention ""id":"123"".

  Scenario: Api would understand an integer id.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"version","id":123}' http://localhost:8042
    Then curl must mention ""id":123".

  Scenario: Api would understand a float id.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"version","id":12.3}' http://localhost:8042
    Then curl must mention ""id":12.3".

  Scenario: Can get the list of tables.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"get-tables","id":1,"params":{"prefix":"test/rand"}}' http://localhost:8042
    Then curl must mention "random_walk".

  Scenario: Can get the list of columns.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"get-columns","id":1,"params":["test/color_of_the_day"]}' http://localhost:8042
    Then curl must mention "odd"
    And curl must mention "color".

  Scenario: Can get some timeseries.
    Given ramen supervisor is started
    And ramen httpd --url http://localhost:8042 --api is started
    When I wait 3 seconds
    And I run curl with arguments --data-binary '{"method":"get-timeseries","id":1,"params":{"since":0,"until":9999999999,"num_points":5,"data":{"test/random_walk":{"select":["x"],"where":[{"lhs":"t","op":">=","rhs":"0"}]}}}}' http://localhost:8042
    Then curl must mention "xyz"

  Scenario: Can set an alert.
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 second
    And I run curl with arguments --data-binary '{"method":"set-alerts","id":1,"params":{"test/random_walk":{"xyz":[{"threshold":42,"recovery":37,"id":"glop"}]}}}' http://localhost:8042
    Then curl must mention ""result":null"

  Scenario: An empty set-alerts must return a valid JSON (non-regression)
    Given ramen httpd --url http://localhost:8042 --api is started
    When I wait 1 seconds
    And I run curl with arguments --data-binary '{"method":"set-alerts","id":1,"params":{}}' http://localhost:8042
    Then curl must mention ""result":null" on stdout
