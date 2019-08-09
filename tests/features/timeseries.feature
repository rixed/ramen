Feature: test ramen timeseries

  Test `ramen timeseries` behavior according to its many options.

  Background:
    Given the whole gang is started
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
    And program test is running
    And I wait 5 second
    And I run ramen with arguments gc

  Scenario: I can obtain some values using timeseries.
    When I run ramen with arguments timeseries -n 5 --since=1000 --until=1005 test/ts v
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

  Scenario: Can extract a time series even when time is inferred from parent.
    Given a file test2.ramen with content
      """
      define f as
        select start as my_start, stop as my_stop, v
        from test/ts;
      """
    And test2.ramen is compiled
    And program test2 is running
    And I wait 5 second
    And I run ramen with arguments gc
    And I run ramen with arguments timeseries -n 5 --since 1000 --until=1005 test2/f v
    Then ramen must print 5 lines on stdout
    And ramen must mention "42"
    And ramen must exit with status 0.
