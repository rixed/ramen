Feature: test ramen tail

  Test `ramen tail` behavior according to its many options.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    # Create a simple sequence generator
    And a file tests/seq.ramen with content
      """
      define gen as select 1 + coalesce(previous.x, 0) as x, 42 as y every 1 second;
      """
    And tests/seq.ramen is compiled
    And ramen supervisor is started

  Scenario: I can retrieve the first 2 lines uting tail.
    Given program tests/seq is running
    When I run ramen with arguments tail --min-seq 0 --max-seq 1 tests/seq/gen
    Then ramen must print 2 lines on stdout
    And after max 3 seconds ramen must exit gracefully.

  Scenario: I can retrieve a given line with a `where` filter.
    Given program tests/seq is running
    When I run ramen with arguments tail --where x=3 --max-seq 4 tests/seq/gen
    Then ramen must print 1 line on stdout
    And after max 5 seconds ramen must exit gracefully.

  Scenario: An unknown field in a `where` filter must trigger an error.
    Given program tests/seq is running
    When I run ramen with arguments tail --where z=0 --max-seq 1 tests/seq/gen
    Then ramen must fail gracefully
    And ramen must mention z on stderr.

  Scenario: Min and max-seqnum options are ANDed with a where expression.
    Given program tests/seq is running
    When I run ramen with arguments tail -w x=3 --min 4 --max 6 tests/seq/gen
    Then after max 1 second ramen must exit gracefully
    And ramen must print no line on stdout.

  Scenario: Min and max-seqnum are inclusive.
    Given program tests/seq is running
    When I run ramen with arguments tail --min 2 --max 2 tests/seq/gen
    Then after max 3 seconds ramen must exit gracefully
    And ramen must print 1 line on stdout.
