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

  Scenario: Ramen sees additional experiments
    When I run ramen with argument variants
    Then ramen must mention "test_external"

  Scenario: We can force a variant of an external experiment
    When I run ramen with argument variants --variant test_external=var2
    Then ramen must mention "var2"

  Scenario: Specifying an unknown variant still raises an error
    When I run ramen with argument variants --variant test_external=nope
    Then ramen must fail gracefully
