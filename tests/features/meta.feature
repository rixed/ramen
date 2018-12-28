Feature: Test some step definitions

  Scenario: We can set a new envvar
    Given the environment variable GLOP is set to PAS_GLOP
    Then the environment variable GLOP must be defined
    And the environment variable GLOP must be set to PAS_GLOP

  Scenario: We can change default envvars
    Given the environment variable RAMEN_VARIANTS is set to GLOP_GLOP
    Then the environment variable RAMEN_VARIANTS must be defined
    And the environment variable RAMEN_VARIANTS must be set to GLOP_GLOP

  Scenario: Some envvars are preset
    Given the environment variable RAMEN_LIBS is set
    Then the environment variable RAMEN_LIBS must be defined
