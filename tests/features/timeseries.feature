Feature: test ramen tail

  Test `ramen timeseries` behavior according to its many options.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    # Create a simple sequence generator
    And a file test.ramen with content
      """
      define ts as
        select now as t, random as v,
               u64(t) % 2 = 1 as odd,
               case when random > 0.66 then "blue"
                    when random > 0.50 then "red"
                    else "green"
               end as color
        every 1 second
        event starts at t;
      """
    And test.ramen is compiled
    And ramen supervisor is started

  Scenario: I can obtain some values using timeseries.
    Given program test is running
    When I run ramen with arguments timeseries -n 5 test/ts v
    Then ramen must print 5 lines on stdout
    And ramen must exit gracefully.
