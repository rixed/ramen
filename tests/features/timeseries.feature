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
        select 1000 + sum globally 1 as start,
               start + 1 as stop,
               42 as v,
               u64(start) % 2 = 1 as odd,
               NULL as n
        every 10 milliseconds;
      """
    And test.ramen is compiled
    And ramen supervisor is started
    And program test is running
    And I wait 3 second
    # timeseries now request a stats file:
    And I run ramen with arguments archivist --no-allocs --no-reconf

  Scenario: I can obtain some values using timeseries.
    When I run ramen with arguments timeseries -n 5 --since 1000 --until=1005 test/ts v
    Then ramen must print 5 lines on stdout
    And ramen must mention "42"
    And ramen must exit gracefully.

  Scenario: No data is not a problem. We still have the times.
    When I run ramen with arguments timeseries -n 5 --since=123 --until=124 test/ts v
    Then ramen must print 5 lines on stdout
    And ramen must not mention "42"
    And ramen must exit with status 0.

  Scenario: One can use --where to filter the output.
    When I run ramen with arguments timeseries -n 6 --since=1000 --until=1006 -w 'odd = true' test/ts odd
    Then ramen must print 6 lines on stdout
    # booleans have been averaged, thus converted into floats:
    And ramen must mention ",1"
    And ramen must exit gracefully.

  Scenario: If all data we have is NULL we still get the times with no values.
    When I run ramen with arguments timeseries --null "<NULL>" -n 5 --since=1000 --until=1005 test/ts n
    Then ramen must print 5 lines on stdout
    And ramen must mention "<NULL>"
    And ramen must not mention ",0"
    And ramen must exit gracefully.
