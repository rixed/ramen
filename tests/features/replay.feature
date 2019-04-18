Feature: test ramen replay in a simple setting

  We first construct a simple environment to test various replaying
  behavior.
  We will have two source functions (s0 and s1, archived) read by a third one
  (r0, non archived).
  We just put a user config to that effect, and first generate the allocs
  file and reconf the workers so that only the first two are archived,
  and then we generate the stats.
  All this is happening in the Background section, which is therefore
  more involved than usual.

  Later tests check `ramen replay`.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_PATH is not defined
    # Speed up reports so archivist do not have to wait for too long:
    And the environment variable RAMEN_REPORT_PERIOD is set to 1
    # Disable initial export so we can check archiving is setup properly:
    And the environment variable RAMEN_INITIAL_EXPORT is set to 0
    And a file ramen_dir/archivist/v5/config with content
      """
      {
        size_limit = 20000000000;
        retentions = {
          "test/s?" => { duration = 600 };
          "test/r0" => { duration = 0 };
        }
      }
      """
    And a file test.ramen with content
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
    # Now we have the workers configured to archive, but we still need some
    # actual data archived before updating the stats:
    And I wait 5 seconds
    And I run ramen with arguments archivist --stats

  Scenario: Check the allocations from the background situation obey the config.
    When I run tr with arguments -d '\n[:blank:]' < ramen_dir/archivist/v5/allocs
    Then tr must mention "","test/r0")=>0"
    And tr must mention "","test/s0")=>10000000000"
    And tr must mention "","test/s1")=>10000000000"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/s0/*/out_ref
    Then cat must mention "archive.b"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/s1/*/out_ref
    Then cat must mention "archive.b"
    When I run cat with arguments ramen_dir/workers/out_ref/*/test/r0/*/out_ref
    Then cat must not mention "archive.b".

  Scenario: Check we can replay s0 (peace of cake).
    # Given we wait 10s before running the archivist we won't have older
    # data in the archive.
    # Also, we need to update the archive data in the stats file:
    When I run ramen with arguments replay test/s0 --since 10 --until 15
    Then ramen must print between 3 and 5 lines on stdout
    And ramen must exit gracefully.

  Scenario: Check we can also replay r0.
    When I run ramen with arguments replay test/r0 --since 10 --until 15
    Then ramen must print between 6 and 10 lines on stdout
    And ramen must exit gracefully.
