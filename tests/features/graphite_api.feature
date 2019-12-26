Feature: Test Ramen Graphite Impersonator from the command line.

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_LIBS is set
    And the environment variable RAMEN_CONFSERVER is set to localhost:29340
    And the environment variable HOSTNAME is set to TEST
    And the environment variable RAMEN_DEBUG is set to 1
    And ramen confserver --insecure 29340 --no-examples is started

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
