Feature: Programs ca  be parameterized

  It is possible to change the behavior of a program with parameters (or
  environment).
  It is also possible to run several instances of the same program with
  different parameters.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    And the environment variable LAST_NAME is set to Smith
    And ramen supervisor is started

  Scenario: A program behavior can depends on parameter and environment.
    Given a file test.ramen with content
      """
      parameter first_name defaults to "Adelaide";
      define f as yield param.first_name ||" "|| env.LAST_NAME AS greeting
        every 1 second;
      """
    And test.ramen is compiled
    And I run ramen with arguments run -p 'first_name="Leontine"' test.x
    When I run ramen with arguments ps
    Then ramen must print "test{first_name="Leontine"}/f"
    When I run ramen with arguments tail --last=-1 'test{first_name="Leontine"}/f' --raw
    Then ramen must print "Leontine Smith".
