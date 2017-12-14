m4_dnl vim: ft=m4
m4_changequote(`[', `]')m4_dnl
m4_changecom()m4_dnl
m4_define([SHELLQUOTE],
	['m4_patsubst([$1], ['], ['"'"'])'])m4_dnl
m4_define([BASE64],
	[m4_esyscmd(echo SHELLQUOTE([$1]) | openssl enc -base64 | tr -d "\n")])m4_dnl
