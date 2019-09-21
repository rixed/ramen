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
    # Create a bad xdep that depends on this to-be-overwriten definition:
    And a file tests/nodep.ramen with content
      """
      define yi as yield "pas glop" as v every 1 second;
      """
    And tests/nodep.ramen is compiled
    And a file tests/dep.ramen with content
      """
      define n as select v from tests/nodep/yi;
      """
    # Ordering of the following two steps is important as dep.x will be
    # moved to xdep.x:
    And tests/dep.ramen is compiled as tests/xdep
    And tests/dep.ramen is compiled
    # Now the real content
    # Notice the type change: but that's OK as supervisor will
    # recompile both dep and nodep:
    And a file tests/nodep.ramen with content
      """
      define yi as yield 1 as v every 1 second;
      """

  Scenario: I can run a worker that depends on nobody.
    Given tests/nodep.ramen is compiled
    And no worker is running
    When I run ramen with arguments run tests/nodep
    Then ramen must exit gracefully
    Then after max 3 seconds worker tests/nodep/yi must be running

  Scenario: I can run a worker alone even if it depends on another one,
            but not without a warning.
    Given tests/dep.ramen is compiled
    And program tests/nodep is not running
    Then program tests/nodep must not be running
    When I run ramen with arguments run tests/dep
    Then ramen must exit gracefully
    Then after max 3 seconds worker tests/dep/n must be running

  Scenario: I can stop any worker nobody depends upon.
    Given programs tests/dep and tests/nodep are running
    When I run ramen with arguments kill tests/dep
    Then ramen must exit gracefully
    Then after max 1 second program tests/dep must not be running
    But after max 3 seconds the program tests/nodep is running

  Scenario: I can also stop a worker that is depended upon,
            but not without a warning.
    Given programs tests/dep and tests/nodep are running
    Then programs tests/dep and tests/nodep must be running
    When I run ramen with arguments kill tests/nodep
    Then ramen must exit gracefully
    Then after max 1 second program tests/nodep must not be running
    But after max 3 seconds the program tests/dep must be running
