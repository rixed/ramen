Feature: Ramen behavior can be customized via experiments

  Let's just check for now that ramen actually sees experiments defined in
  an external file. This file is supposed to go into
  $RAMEN_DIR/experiments/$EXP_VERSION/config.

  Background:
    Given a file ramen_dir/experiments/v1/config with content
      """
      {
        "test_external" => { "var1" => { descr = "the first variant"; share = 0 };
                             "var2" => { descr = "the second variant"; share = 1 } }
      }
      """
    And a file test_prog.ramen with content
      """
      RUN IF (variant("test_external") = "var1") |? false;
      DEFINE f AS YIELD "running" AS glop every 500ms;
      """
    And the environment variable RAMEN_COLORS is set

  Scenario: Ramen sees additional experiments
    When I run ramen with argument variants
    Then ramen must mention "test_external"
    And ramen must mention "var2 (SELECTED) (100%)"

  Scenario: We can force a variant of an external experiment
    When I run ramen with argument variants --variant test_external=var1
    Then ramen must mention "var1 (SELECTED) (0%)"

  Scenario: Specifying an unknown variant still raises an error
    When I run ramen with argument variants --variant test_external=nope
    Then ramen must fail gracefully

  Scenario: A function might run or not depending on some experiment (1)
    Given the environment variable RAMEN_VARIANTS is set to test_external=var1
    And the whole gang is started
    And test_prog.ramen is compiled
    And the program test_prog is running
    When I wait 2 second
    And I run ramen with arguments tail -n 1 test_prog/f
    Then ramen must mention "running"

  Scenario: A function might run or not depending on some experiment (2)
    Given the environment variable RAMEN_VARIANTS is set to test_external=var2
    And the whole gang is started
    And test_prog.ramen is compiled
    And the program test_prog is running
    When I wait 2 second
    And I run timeout with arguments 1 ramen tail -n 1 test_prog/f
    Then timeout must not mention "running"
