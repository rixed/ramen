Feature: behavior under harsh conditions

  Check that ramen keeps behaving sensibly under adversary conditions.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And ramen supervisor --autoreload=1 --debug --fail-for-good is started
    Given a file dir/p1.ramen with content
      """
      define f as select "one" as v every 1s;
      """
    And a file p2.ramen with content
      """
      define f as select "hello" as v every 1s;
      """
    And dir/p1.ramen and p2.ramen are compiled
    And programs dir/p1 and p2 are running
    And I wait 3 seconds.

  Scenario: ramen will recompile modified programs
    When I run ramen with arguments tail -n 1 dir/p1/f
    Then ramen must mention "one" on stdout
    Given a file dir/p1.ramen with content
      """
      define f as select "two" as v every 1s;
      """
    And I wait 4 seconds
    Then program dir/p1 must be running
    And I run ramen with arguments tail -n 1 dir/p1/f
    Then ramen must mention "two" on stdout.

  Scenario: ramen is fine with deleted programs
    Given I run rm with arguments dir/*
    And I run rmdir with argument dir
    When I run ramen with argument ps
    Then ramen must mention "p2/f"
    When I run ramen with arguments tail -n 1 p2/f
    Then ramen must mention "hello" on stdout
    And ramen must exit gracefully
    When I wait 2 seconds
    Then ramen supervisor --autoreload=1 --debug --fail-for-good must still be running.
