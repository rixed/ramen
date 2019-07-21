Feature: users are authenticated

  Background:
    Given ramen must be in the path
    And the environment variable RAMEN_PATH is not defined
    And a file ramen_dir/confserver/public_key with content
      """
      rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7
      """
    And a file ramen_dir/confserver/private_key with content
      """
      JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6
      """
    And a file ramen_dir/confserver/users/TESTER with content
      """
      {
        roles = [ User ];
        clt_pub_key = "Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID"
      }
      """
    And the environment variable USER is set to TESTER
    And a file client_pub with content
      """
      Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID
      """
    And a file client_priv with content
      """
      D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs
      """
    And a file client_pub_other with content
      """
      S)*gt?>k4?lz$vsS7%#-2No7TF2s@:h6t^Ll#g{+
      """
    And a file client_priv_other with content
      """
      )UW^6dz&<u?S&z^bxLiRAjp)^gViV/OS5a}+XgVU
      """
    And a file client_priv_bad with content
      """
      NotTheProperPrivateKeyButLetMeInWouldYou
      """
    And I run chmod with arguments go-rwx ramen_dir/confserver/private_key client_priv client_priv_bad
    And ramen confserver --secure 29341 is started

  Scenario: user can connect with the proper public key
    When I run ramen with arguments ps --confserver localhost:29341 --confserver-key ramen_dir/confserver/public_key --pub-key client_pub --priv-key client_priv
    Then ramen must exit gracefully

#  As much as ZMQ API for basic connect/read/write is simple, the API to get beyond that
#  point gets hairy quickly (authentication API is a pain, error detection is complex,
#  instrumentation is close to impossible...)
#  FIXME: get rid of ZMQ.
#  Scenario: user can not connect without the proper keys
#    When I run ramen with arguments ps --confserver localhost:29341 --confserver-key ramen_dir/confserver/public_key --pub-key client_pub_other --priv-key client_pub_other
#    Then ramen must fail gracefully
#
#  Scenario: user can not connect with invalid keys
#    When I run ramen with arguments ps --confserver localhost:29341 --confserver-key ramen_dir/confserver/public_key --pub-key client_pub --priv-key client_priv_bad
#    Then ramen must fail gracefully
#
#  Scenario: user can not connect without any key at all
#    When I run ramen with arguments ps --confserver localhost:29341
#    Then ramen must fail gracefully
