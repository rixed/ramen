Feature: test ramen tail

  Test `ramen tail` behavior according to its many options.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    # Create a simple sequence generator
    And a file test.ramen with content
      """
      define gen as
        select
          1 + (previous.x |? 0) as x,
          42 as y,
          "blue" as _blue,
          case when random > 0.66 then _blue
               when random > 0.50 then "red"
               else "green"
          end as color
        every 10 milliseconds;
      """
    And test.ramen is compiled
    And ramen supervisor is started
    And program test is running
    And I wait 3 seconds

  Scenario: I can retrieve the first 2 lines using tail.
    When I run ramen with arguments tail --min-seq 0 --max-seq 1 test/gen
    Then ramen must print 2 lines on stdout
    And ramen must mention "42"
    And ramen must exit gracefully.

  Scenario: I can retrieve a given line with a `where` filter.
    When I run ramen with arguments tail --where x=3 --max-seq 4 test/gen
    Then ramen must print 1 line on stdout
    And ramen must mention "42"
    And ramen must exit gracefully.

  Scenario: An unknown field in a `where` filter must trigger an error.
    When I run ramen with arguments tail --where z=0 --max-seq 1 test/gen
    Then ramen must fail gracefully
    And ramen must mention "z" on stderr.

  Scenario: Min and max-seqnum options are ANDed with a where expression.
    When I run ramen with arguments tail -w x=3 --min 4 --max 6 test/gen
    Then ramen must exit gracefully
    And ramen must print no line on stdout.

  Scenario: Two where options are ANDed.
    When I run ramen with arguments tail -w x=3 -w y=42 --max 6 test/gen
    Then ramen must exit gracefully
    And ramen must print 1 line on stdout
    And ramen must mention "42".

  Scenario: Min and max-seqnum are inclusive.
    When I run ramen with arguments tail --min 2 --max 2 test/gen
    Then ramen must exit gracefully
    And ramen must print 1 line on stdout
    And ramen must mention "42".

  Scenario: We can filter on string columns
    When I run ramen with arguments tail -w 'color != "blue"' test/gen
    Then ramen must exit gracefully
    And ramen must not mention "blue"
    And ramen must mention "42".

  Scenario: We can select several values with the in operator.
    When I run ramen with arguments tail -w 'color in ["blue";"red"]' test/gen
    Then ramen must exit gracefully
    And ramen must not mention "green"
    And ramen must mention "42".

  Scenario: Headers must not show private fields.
    When I run ramen with arguments tail -h --min 2 --max 2 test/gen
    Then ramen must exit gracefully
    And ramen must print 2 lines on stdout
    And ramen must not mention "_blue".

  Scenario: Private fields cannot be used as filter.
    When I run ramen with arguments tail -h --min 2 --max 2 test/gen -w '_blue = "red"'
    Then ramen must fail gracefully
    And ramen must mention "_blue" on stderr.

  Scenario: Once can create transient functions from the command line.
    When I run ramen with arguments tail -n 3 -h -- select y+1 AS z from test/gen
    Then ramen must print more than 4 lines on stdout
    And ramen must mention "43"
    And ramen must exit gracefully.
