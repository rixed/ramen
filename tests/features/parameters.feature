Feature: Programs can be parameterized

  It is possible to change the behavior of a program with parameters (or
  environment).
  It is also possible to run several instances of the same program with
  different parameters.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_PATH is not defined
    And the environment variable LAST_NAME is set to Smith
    And a file test.ramen with content
      """
      parameter first_name defaults to "Adelaide";
      define f as yield param.first_name ||" "|| env.LAST_NAME AS greeting
        every 1 second;
      """
    And test.ramen is compiled
    And ramen supervisor is started

  Scenario: A program behavior can depends on parameter and environment.
    Given I run ramen with arguments run -p 'first_name="Leontine"' test.x --as test/Leontine
    And I wait 2 seconds
    # ...for the stats to arrive
    When I run ramen with arguments ps
    Then ramen must mention "test/Leontine/f"
    When I run ramen with arguments tail -n 1 'test/Leontine/f' --raw
    Then ramen must mention "Leontine Smith".

  Scenario: We can run two instances of a program with different parameters.
    Given I run ramen with arguments run -p 'first_name="Romuald"' test.x --as test/Romuald
    And I run ramen with arguments run -p 'first_name="Raphael"' test.x --as test/Raphael
    And I wait 2 seconds
    # ...for the stats to arrive
    When I run ramen with arguments ps
    Then ramen must mention "test/Romuald/f"
    And ramen must mention "test/Raphael/f".
    When I run ramen with arguments _expand 'test.*'
    Then ramen must mention "Romuald"
    And ramen must mention "Raphael".

  Scenario: But only one under the same name.
    Given I run ramen with arguments run -p 'first_name="Josephine"' test.x --as test/Josephine
    And I run ramen with arguments run -p 'first_name="Josephine"' test.x --as test/Josephine
    And I wait 2 seconds
    # ...for the stats to arrive
    When I run ramen with arguments ps
    Then ramen must print 1 line on stdout.

  Scenario: passing an unknown parameter is an error.
    When I run ramen with arguments run -p 'last_name="Doe"' test.x
    Then ramen must fail gracefully.
