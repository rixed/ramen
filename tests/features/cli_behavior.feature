Feature: check that the CLI behave in a sensible way

  Background:
    Given the whole gang is started
    And a file tests/p.ramen with content
      """
      define f as yield "glop" as glop every 1 second;
      """
    And tests/p.ramen is compiled

  Scenario: I can run a worker that depends on nobody.
    Given the program tests/p is running
    When I run ramen with arguments ps
    Then ramen must mention "tests/p"
    When I run ramen with arguments kill tests/p
    Then ramen must exit gracefully
    When I run ramen with arguments ps
    Then ramen must not mention "tests/p"
