Feature: Test Ramen Graphite Impersonator for the metrics API.

  There are two subcommands to this API: /metrics/find and /metrics/expand.
  Grafana does not use /metrics/expand therefore it is not implemented for
  now.

  The goal of /metrics/find is to autocomplete a single component of a path,
  that can contain globs '*'. but those globs must not be replaced. So for
  instance, the answer to "a.*.c.*" is the list: [ "a.*.c.m1.", "a.*.c.m2" ],
  where m1 is not a leaf but m2 is (notice the final dot after m1).

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_BUNDLE_DIR is set
    And a file p1/p2/t1.ramen with content
      """
      define f1 as
        yield
          now as start, -- need an event time for Graphite
          sum globally 1 as seq,
          seq % 10 as digit,
          case when digit = 0 then "cyan"
               when digit = 1 then "magenta"
               when digit = 2 then "yellow"
               else "black"
          end as color,
          random as high_card
        every 10 milliseconds
        factors digit, color;
      """
    And a file t2.ramen with content
      """
      define f2 as
        yield now as start, 42 as the_answer every 99999h;
      """
    And p1/p2/t1.ramen and t2.ramen are compiled
    # The above program needs to run for the factor possible values to be known
    And ramen supervisor is started
    And programs p1/p2/t1 and t2 are running
    And ramen httpd --url http://localhost:8042/ --graphite is started
    And I wait 3 seconds

  Scenario: Completing nothing should yield the first possible program names, "p1." and "t2.",
            both expandable as even t2 has numeric data fields.
    When I run curl with arguments http://localhost:8042/metrics/find?query=*
    Then curl must mention ""text":"p1""
    And curl must mention ""text":"t2""

  Scenario: Completing p1 should yield "p2"
    When I run curl with arguments http://localhost:8042/metrics/find?query=p1.*
    Then curl must mention ""text":"p2""

  Scenario: Completing p1.p2.t1.f1 should yield the possible values for digits
    When I run curl with arguments http://localhost:8042/metrics/find?query=p1.p2.t1.f1.*
    Then curl must mention ""text":"1""

  Scenario: Completing *.*.*.f1 should return the same as above
    When I run curl with arguments http://localhost:8042/metrics/find?query=*.*.*.f1.*
    Then curl must mention ""text":"1""

  Scenario: Completing p1.p2.t1.f1.0 should yield the possible values for colors
    When I run curl with arguments http://localhost:8042/metrics/find?query=p1.p2.t1.f1.0.*
    Then curl must mention ""text":"magenta""

  Scenario: By the way, filters could be quoted and ramen should unquote them.
    When I run curl with arguments http://localhost:8042/metrics/find?query="p1".p2."t1".f1.0.*
    Then curl must mention ""text":"magenta""

  Scenario: Finally, completing that should yield the list of data field such
            as high_card, that are all leaves.
    When I run curl with arguments http://localhost:8042/metrics/find?query=p1.p2.t1.f1.0.magenta.*
    Then curl must mention ""text":"high_card""
    And curl must mention ""leaf":1"
    And curl must not mention ""leaf":0"
