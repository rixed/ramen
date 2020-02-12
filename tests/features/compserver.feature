Feature: It is possible to compile a program via the confserver

  Background:
    Given ramen must be in the path
    And a file testme.ramen with content
      """
      define f as
        yield "hello world" as greetings,
        now as start, start as stop, random as v every 1s;
      """
    And a file children/child.ramen with content
      """
      define c as select greetings || "!" as warmer_greetings from ../../testme/f;
      """
    And a file test.alert with content
      """
      V1 {
        table = "testme/f";
        column = "v";
        alert = {
          threshold = 0.9;
          recovery = 0.8;
        };
      }
      """
    And ramen confserver --insecure 29341 is started
    And ramen precompserver --confserver localhost:29341 is started
    And the environment variable USER is set to TESTER

  Scenario: Local files can be compiled via confserver
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    Then ramen must mention "compiled (TODO)"
    And ramen must exit gracefully
    When I run ramen with arguments compile --confserver localhost:29341 test.alert
    Then ramen must mention "compiled (TODO)"
    And ramen must exit gracefully

  Scenario: Relative parent resolution happens via the source tree (failure mode)
    When I run ramen with arguments compile --confserver localhost:29341 children/child.ramen
    Then ramen must mention "err:"Cannot find parent source testme""
    And ramen must fail gracefully

  Scenario: Relative parent resolution happens via the source tree (success)
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    And I run ramen with arguments compile --confserver localhost:29341 children/child.ramen
    Then ramen must mention "compiled (TODO)"
    And ramen must exit gracefully
