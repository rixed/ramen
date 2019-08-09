Feature: test ramen tail

  Test `ramen tail` behavior according to its many options.

  Background:
    Given the whole gang is started
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
        every 1s;
      """
    And test.ramen is compiled
    And program test is running

  Scenario: I can retrieve the next 2 lines using tail.
    When I run ramen with arguments tail -n 2 test/gen
    Then ramen must print 2 lines on stdout
    And ramen must exit gracefully.

  Scenario: Asking for a huge last count does not wait for future lines:
    When I run ramen with arguments tail -l 99999999 test/gen
    And ramen must exit gracefully.

  Scenario: I can retrieve a given line with a `where` filter.
    When I run ramen with arguments tail --where x=4 -n 1 test/gen
    Then ramen must print 1 line on stdout
    And ramen must mention "42"
    And ramen must exit gracefully.

  Scenario: An unknown field in a `where` filter must trigger an error.
    When I run ramen with arguments tail --where z=0 -n 1 test/gen
    Then ramen must fail gracefully
    And ramen must mention "z" on stderr.

  Scenario: Two where options are ANDed.
    When I run ramen with arguments tail -w x=4 -w y=42 -n 1 test/gen
    Then ramen must exit gracefully
    And ramen must print 1 line on stdout
    And ramen must mention "42".

  Scenario: We can filter on string columns
    When I run ramen with arguments tail -w 'color != "blue"' -n 1 test/gen
    Then ramen must exit gracefully
    And ramen must not mention "blue"
    And ramen must mention "42".

  Scenario: We can select several values with the in operator.
    When I run ramen with arguments tail -w 'color in ["blue";"red"]' -n 1 test/gen
    Then ramen must exit gracefully
    And ramen must not mention "green"
    And ramen must mention "42".

  Scenario: It is OK to ask just for the headers.
    When I run ramen with arguments tail -h -l 0 test/gen
    Then ramen must exit gracefully
    And ramen must print 1 lines on stdout
    And ramen must mention "color".

  Scenario: Headers must not show private fields.
    When I run ramen with arguments tail -h -n 1 test/gen
    Then ramen must exit gracefully
    And ramen must print 2 lines on stdout
    And ramen must not mention "_blue".

  Scenario: Private fields cannot be used as filter.
    When I run ramen with arguments tail -h -w '_blue = "red"' test/gen
    Then ramen must fail gracefully
    And ramen must mention "_blue" on stderr.

# TODO: support for immediate code with confserver
#  Scenario: Once can create transient functions from the command line.
#    When I run ramen with arguments tail -n 3 -h -- select y+1 AS z from test/gen
#    Then ramen must print more than 4 lines on stdout
#    And ramen must mention "43"
#    And ramen must exit gracefully.
#
#  Scenario: this works also when the function is in a single argument.
#    When I run ramen with arguments tail -n 3 -h -- 'select y+1 AS z from test/gen'
#    Then ramen must print more than 4 lines on stdout
#    And ramen must mention "43"
#    And ramen must exit gracefully.
