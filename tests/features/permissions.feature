Feature: Test ramen user permissions.

  Background:
    Given the whole gang is started
    And a file test.ramen with content
      """
      define f as
        yield 1 as p every 1s;
      """
    And test.ramen is compiled

  Scenario: A user with user perm can run a program.
    Given user foo is defined with user perms
    When I run ramen with arguments run test --identity identity --priv-key priv --pub-key pub
    Then after max 5 seconds ramen must exit gracefully

  Scenario: A user with admin perm can run a program.
    Given user foo is defined with admin perms
    When I run ramen with arguments run test --identity identity --priv-key priv --pub-key pub
    Then after max 5 seconds ramen must exit gracefully

