Feature: It is possible to compile a program via the confserver

  Background:
    Given ramen must be in the path
    And a file testme.ramen with content
      """
      define f as yield "hello world" as greetings every 1s;
      """
    And ramen confserver --port 29341 is started
    And ramen compserver --confserver localhost:29341 is started

  Scenario: Local file can be compiled via confserver
    When I run ramen with arguments compile --confserver localhost:29341 testme.ramen
    Then ramen must mention "TODO"
    And ramen must exit gracefully
