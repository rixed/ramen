Feature: check that the CLI behave in a sensible way

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    And a file tests/p.ramen with content
      """
      define f as yield "glop" as glop every 1 second;
      """
    And tests/p.ramen is compiled

  Scenario: I can run a worker that depends on nobody.
    Given ramen supervisor is started
    And the program tests/p is running
    When I run ramen with arguments ps
    Then ramen must mention "tests/p"
    When I run ramen with arguments kill tests/p
    Then ramen must exit gracefully
    When I run ramen with arguments ps
    Then ramen must not mention "tests/p"
    When I run ramen with arguments ps --all
    Then ramen must mention "tests/p"
    When I run ramen with arguments kill --purge tests/p
    Then ramen must exit gracefully
    When I run ramen with arguments ps --all
    Then ramen must not mention "tests/p"
