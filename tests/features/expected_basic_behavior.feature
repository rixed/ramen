Feature: The Ramen cli behaves according to common expectations.

  Here we just check that nothing unexpected happens when one runs the
  executable in conventional ways.

  Background:
    Given ramen must be in the path

  Scenario: Nothing bad happens if I just run ramen.
    When I run ramen with no argument
    Then ramen must print some lines on stdout
    But ramen must print no line on stderr
    And ramen must exit with status 0

  Scenario: I can get some help with just --help.
    When I run ramen with argument --help
    Then ramen must print some lines on stdout
    But ramen must print no line on stderr
    And ramen must exit with status 0

  Scenario Outline: Some help is printed for each subcommand.
    When I run ramen with arguments <subcommand> --help
    Then ramen must print a lot of lines on stdout
    But ramen must print no line on stderr
    And ramen must exit with status 0

    Examples:
      | subcommand |
      | supervisor |
      | notifier   |
      | notify     |
      | compile    |
      | run        |
      | kill       |
      | ps         |
      | tail       |
      | timeseries |
      | timerange  |
      | httpd      |
      | gc         |
      | links      |
      | archivist  |
