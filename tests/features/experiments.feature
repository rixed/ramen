Feature: Ramen behavior can be customized via experiments

  Let's just check for now that ramen actually sees experiments defined in
  an external file. This file is supposed to go into
  $RAMEN_PERSIST_DIR/experiments/$EXP_VERSION/config.

  Background:
    Given ramen must be in the path
    And a file ramen_persist_dir/experiments/v1/config with content
      """
      {
        "test_external" => { "var1" => { descr = "the first variant" } ;
                             "var2" => { descr = "the second variant" } }
      }
      """
    And a file test_prog.ramen with content
      """
      RUN IF (variant("test_external") = "var1") |? false;
      DEFINE f AS YIELD "running" AS glop every 500ms;
      """
    And test_prog.ramen is compiled

  Scenario: Ramen sees additional experiments
    When I run ramen with argument variants
    Then ramen must mention "test_external"

  Scenario: We can force a variant of an external experiment
    When I run ramen with argument variants --variant test_external=var2
    Then ramen must mention "var2"

  Scenario: Specifying an unknown variant still raises an error
    When I run ramen with argument variants --variant test_external=nope
    Then ramen must fail gracefully

  Scenario: A function might run or not depending on some experiment (1)
    Given the environment variable RAMEN_VARIANTS is set to test_external=var1
    And the program test_prog is running
    And ramen supervisor is started
    When I wait 2 second
    And I run ramen with arguments tail -n 1 test_prog/f
    Then ramen must mention "running"

  Scenario: A function might run or not depending on some experiment (2)
    Given the environment variable RAMEN_VARIANTS is set to test_external=var2
    And the program test_prog is running
    And ramen supervisor is started
    When I wait 2 second
    And I run timeout with arguments 1 ramen tail -n 1 test_prog/f
    Then timeout must not mention "running"
