Feature: hot reloading of ramen workers

  Test `ramen supervisor` behavior with regard to updating running workers.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_PATH is not defined
    And the environment variable RAMEN_CONFSERVER is not defined
    # Create two simple programs forming a chain of 3 workers;
    And a file p1.ramen with content
      """
      define o1 as select 1 as v every 1 second;
      """
    And a file p2.ramen with content
      """
      define o1 as
        from p1/o1 select v,
          case
            when v = 1 then "one"
            when v = 2 then "two"
            else "bigger"
          end as n;

      define o2 as select n from o1;
      """
    And p1.ramen and p2.ramen are compiled
    And ramen supervisor --autoreload=1 is started
    And programs p1 and p2 are running.

  Scenario: Without any more command some output must be visible.
    # Use min/max-seq instead of -n cause min/max-seq will wait for those
    # tuples while -n would just exit early.
    When I run ramen with arguments tail --max-seq=0 p2/o2
    Then ramen must mention "one" on stdout.

  Scenario: A program can be changed and, as types permit, will be reconnected
    to its relatives.
    Given a file p1_2.ramen with content
      """
      define o1 as select 2 as v every 1 second;
      """
    And p1_2.ramen is compiled as p1
    And I wait 3 seconds
    When I run ramen with arguments tail -l 1 p2/o2
    Then ramen must mention "two" on stdout.
