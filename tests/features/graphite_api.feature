Feature: Test Ramen Graphite Impersonator from the command line.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_CONFSERVER is not defined

  Scenario: If must be possible to get a graphite version
    Given ramen httpd --url http://localhost:8042 --graphite is started
    When I wait 1 second
    And I run curl with arguments http://localhost:8042/version
    Then curl must mention "1.1.3"

  Scenario: It doesn't matter if the URL has some extra slashes
    Given ramen httpd --url http://localhost:8042 --graphite is started
    When I wait 1 second
    And I run curl with arguments http://localhost:8042//version
    Then curl must mention "1.1.3"

  Scenario: It doesn't matter if the URL prefix has some trailing slashes
    Given ramen httpd --url http://localhost:8042/ --graphite is started
    When I wait 1 second
    And I run curl with arguments http://localhost:8042/version
    Then curl must mention "1.1.3"
