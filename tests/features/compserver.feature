Feature: It is possible to compile a program via the confserver

  Background:
    Given ramen must be in the path
    And a file testme.ramen with content
      """
      define f as
        yield "hello world" as greetings,
        now as _t, _t as start, _t as stop, random as v every 1s;
      """
    And a file children/child.ramen with content
      """
      define c as select greetings || "!" as warmer_greetings from ../../testme/f;
      """
    And a file test.alert with content
      """
      {
        "table": "testme/f",
        "column": "v",
        "threshold": { "Constant": 0.9 },
        "hysteresis": -0.2
      }
      """
    And ramen confserver --debug --insecure 29341 --no-examples is started
    And ramen precompserver --debug --confserver localhost:29341 is started
    And the environment variable USER is set to TESTER

  Scenario: Local files can be compiled via confserver
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    Then ramen must mention "Program testme is compiled"
    And ramen must exit gracefully
    When I run ramen with arguments compile --confserver localhost:29341 test.alert
    Then ramen must mention "Program test is compiled"
    And ramen must exit gracefully

  Scenario: Relative parent resolution happens via the source tree (failure mode)
    When I run ramen with arguments compile --debug --confserver localhost:29341 children/child.ramen
    Then ramen must mention "Cannot find parent source testme" on stderr
    And ramen must fail gracefully

  Scenario: Relative parent resolution happens via the source tree (success)
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    And I run ramen with arguments compile --confserver localhost:29341 children/child.ramen
    Then ramen must mention "Program children/child is compiled"
    And ramen must exit gracefully

  Scenario: Compilations are cached
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    And I run ramen with arguments stats --colors=never precompilations_count
    Then ramen must mention "ok -> 1"
    And ramen must exit gracefully
    When I run ramen with arguments compile --confserver localhost:29341 --replace testme.ramen --debug
    And I run ramen with arguments stats --colors=never precompilations_count
    Then ramen must mention "ok -> 1"
    And ramen must mention "cached -> 1"
    And ramen must exit gracefully
