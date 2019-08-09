Feature: I can compile programs and get proper error status.

  One can compile program files locally, given proper setting of the ramen root
  to find dependencies. Also, the outcome of the compilation is reflected on
  ramen exit code.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_CONFSERVER is not defined
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_PATH is not defined
    And a file tests/nodep1.ramen with content
      """
      define yi1 as yield 1 as v every 1 second;
      """
    And a file tests/nodep2.ramen with content
      """
      define yi2 as yield 2 as v every 2 seconds;
      """
    And a file tests/dep1.ramen with content
      """
      define n1 as select v from tests/nodep1/yi1;
      """
    And no files ending with .x are present in tests

  Scenario: I get an error when compiling an unknown file.
    Given no file named unknown.ramen is present in tests
    When I run ramen with arguments compile -L . tests/unknown.ramen
    Then ramen must exit with status not 0
    And ramen must print a few lines on stderr

  Scenario: One can compile files that have no dependency.
    When I run ramen with arguments compile -L . tests/nodep*.ramen
    # This we call 'ramen produces executable files ...' from now on:
    Then ramen must print a few lines on stdout
    But ramen must print no line on stderr
    And ramen must exit with status 0
    And executable files tests/nodep1.x and tests/nodep2.x must exist

  Scenario: One can not compile a file with missing dependency.
    Given no file named tests/nodep1.x is present in tests
    When I run ramen with arguments compile -L . tests/dep1.ramen
    Then ramen must fail gracefully

  Scenario: One can compile a file which dependency is compiled.
    When I run ramen with arguments compile -L . tests/nodep1.ramen
    And I run ramen with arguments compile -L . tests/dep1.ramen
    Then ramen must produce executable file tests/dep1.x

  Scenario: One can compile a file and its dependency in one go.
    At least if specifying a dependency first, as expected from a compiler.
    When I run ramen with arguments compile -L . tests/nodep1.ramen tests/dep1.ramen
    Then ramen must produce executable files tests/nodep1.x and tests/dep1.x

  Scenario: One can compile a file with no more indications than path.
    When I run ramen with arguments compile tests/nodep1.ramen
    Then ramen must produce executable file tests/nodep1.x
