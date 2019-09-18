Feature: We can run and kill any program in any order

  Regardless of interrelationships, any compiled program can be run and killed
  at any point in time.
  When starving a child or stalling a parent, a warning should be issued though.
  A command line option should prevent the above to happen.

  Ramen should refuses to run a new operation which in or out types are
  incompatible with its environment.

  When a running program is recompiled, the new version can be autoloaded.

  Background:
    Given the whole gang is started
    And a file tests/nodep.ramen with content
      """
      define yi as yield "pas glop" as v every 1 second;
      """
    And tests/nodep.ramen is compiled
    And a file tests/dep.ramen with content
      """
      define n as select v from tests/nodep/yi;
      """

  Scenario: I can run a worker that depends on nobody.
    Given tests/nodep.ramen is compiled
    And no worker is running
    When I run ramen with arguments run --as tests/nodep tests/nodep
    Then ramen must exit gracefully
    Then after max 3 seconds worker tests/nodep/yi must be running

  Scenario: I can not compile a program which depends on some unknown parent.
    Given program tests/nodep is not running
    When I run ramen with arguments compile tests/dep.ramen
    Then ramen must fail gracefully
    And ramen must mention "tests/nodep/yi"

  Scenario: I can compile this program as soon as the parent is running.
    Given program tests/nodep is running
    When I run ramen with arguments compile tests/dep.ramen
    Then ramen must exit gracefully

  Scenario: I can also stop a worker that is depended upon,
            but not without a warning.
    Given program tests/nodep is running
    And tests/dep.ramen is compiled
    And program tests/dep is running
    Then programs tests/dep and tests/nodep must be running
    When I run ramen with arguments kill tests/nodep
    Then ramen must exit gracefully
    Then after max 1 second program tests/nodep must not be running
    But after max 3 seconds the program tests/dep must be running
