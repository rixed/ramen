Feature: We can run and kill any program in any order

  Regardless of interrelationships, any compiled program can be run and killed
  at any point in time.
  When starving a child or stalling a parent, a warning should be issued though.
  A command line option should prevent the above to happen.

  Ramen should refuses to run a new operation which in or out types are
  incompatible with its environment.

  When a running program is recompiled, the new version can be autoloaded.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And the environment variable RAMEN_ROOT is not defined
    # Create a bad xdep that depends on this to-ne-overwriten definition:
    And a file tests/nodep.ramen with content
      """
      define yi as yield "pas glop" as v every 1 second;
      """
    And tests/nodep.ramen is compiled
    And a file tests/dep.ramen with content
      """
      define n as select v from tests/nodep/yi;
      """
    And tests/dep.ramen is compiled as tests/xdep.x
    # Now the real content
    And a file tests/nodep.ramen with content
      """
      define yi as yield 1 as v every 1 second;
      """
    And ramen supervisor is started

  Scenario: I can run a worker that depends on nobody.
    Given tests/nodep.ramen is compiled
    And no worker is running
    When I run ramen with arguments run tests/nodep.x
    Then ramen must exit gracefully
    Then after max 1 second worker tests/nodep/yi must be running

  Scenario: I can run a worker alone even if it depends on another one,
            but not without a warning.
    Given tests/dep.ramen is compiled
    And program tests/nodep is not running
    Then program tests/nodep must not be running
    When I run ramen with arguments run tests/dep.x
    Then ramen must exit with status 0
    And ramen must print a few lines on stderr
    Then after max 1 second worker tests/dep/n must be running

  Scenario: I can stop any worker nobody depends upon.
    Given programs tests/dep and tests/nodep are running
    When I run ramen with arguments kill tests/dep
    Then ramen must exit gracefully
    Then after max 1 second program tests/dep must not be running
    But the program tests/nodep is running

  Scenario: I can also stop a worker that is depended upon,
            but not without a warning.
    Given programs tests/dep and tests/nodep are running
    Then programs tests/dep and tests/nodep must be running
    When I run ramen with arguments kill tests/nodep
    Then ramen must exit with status 0
    And ramen must print a few lines on stderr
    Then after max 1 second program tests/nodep must not be running
    But the program tests/dep must be running
