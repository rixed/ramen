Feature: test the archivist

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_PATH is not defined
    # Speed up reports so archivist do not have to wait for too long:
    And the environment variable RAMEN_REPORT_PERIOD is set to 1
    # Disable initial export so we can check archiving is setup properly:
    And the environment variable RAMEN_INITIAL_EXPORT is set to 0

  Scenario: Default configuration must be to archive the root only
    Given a file test.ramen with content
      """
      define s0 as yield (previous.start |? 0) + 1 as start,
                         (previous.x |? 0) + 2 as x every 1s;
      define s1 as yield (previous.start |? 0) + 1 as start,
                         (previous.x |? 1) + 2 as x every 1s;
      define r0 as select * from s0, s1;
      """
    And test.ramen is compiled
    And ramen supervisor is started
    And program test is running
    And I wait 10 seconds
    And I run ramen with arguments archivist --stats --allocs --reconf
    And I run tr with arguments -d '\n[:blank:]' < ramen_dir/archivist/v6/allocs
    Then tr must mention "","test/r0")=>0"
    And tr must mention "","test/s0")=>536870912"
    And tr must mention "","test/s1")=>536870912"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/s0/*/out_ref
    Then cat must mention "archive.b"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/s1/*/out_ref
    Then cat must mention "archive.b"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/r0/*/out_ref
    Then cat must not mention "archive.b".
