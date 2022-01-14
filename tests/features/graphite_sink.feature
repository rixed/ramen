Feature: Test Ramen Graphite data sink

  Background:
    Given the whole gang is started

  Scenario: Graphite metric should be decoded properly and visible in tail
    And a file test.ramen with content
      """
      define graphite as listen for graphite;
      """
    And test.ramen is compiled
    And program test is running
    And I wait 3 seconds
    Given echo "foo.bar;cpu=0;host=glop 42 1545202428" | socat -t 0 STDIN UDP:127.0.0.1:2003 is run every second
    And I run ramen with arguments tail -n 1 test/graphite
    Then ramen must print 1 line on stdout
    And ramen must mention "foo.bar"
    And ramen must mention "("cpu";"0")"
    And ramen must mention "("host";"glop")"
    And ramen must mention "42"
    And ramen must mention "1545202428"
    And ramen must exit gracefully.

  Scenario: Same test as above but on TCP rather than UDP
    And a file test.ramen with content
      """
      define graphite as listen for graphite PROTO TCP;
      """
    And test.ramen is compiled
    And program test is running
    And I wait 3 seconds
    Given echo "foo.bar;cpu=0;host=glop 42 1545202428" | socat -t 0 STDIN TCP:127.0.0.1:2003 is run every second
    And I run ramen with arguments tail -n 1 test/graphite
    Then ramen must print 1 line on stdout
    And ramen must mention "foo.bar"
    And ramen must mention "("cpu";"0")"
    And ramen must mention "("host";"glop")"
    And ramen must mention "42"
    And ramen must mention "1545202428"
    And ramen must exit gracefully.
