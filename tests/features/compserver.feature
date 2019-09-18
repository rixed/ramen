Feature: It is possible to compile a program via the confserver

  Background:
    Given the whole gang is started
    And a file testme.ramen with content
      """
      define f as yield "hello world" as greetings every 1s;
      """
    And a file children/child.ramen with content
      """
      define c as select greetings || "!" as warmer_greetings from ../../testme/f;
      """

  Scenario: Local file can be compiled via confserver
    When I run ramen with arguments compile testme.ramen
    Then ramen must mention "compiled (TODO)"
    And ramen must exit gracefully

  Scenario: Relative parent resolution happens via the source tree (failure mode)
    When I run ramen with arguments compile children/child.ramen
    Then ramen must mention "err:"Cannot find parent program testme""
    And ramen must fail gracefully

  Scenario: Relative parent resolution happens via the running programs (success)
    When I run ramen with arguments compile testme.ramen
    And I run ramen with arguments run testme
    And I run ramen with arguments compile children/child.ramen
    Then ramen must mention "compiled (TODO)"
    And ramen must exit gracefully
