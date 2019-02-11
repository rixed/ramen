set terminal svg enhanced
set datafile separator ','

set output "RamenVsKsql_cpu.svg"
set title "Cumulative consumed %CPU"
plot 'RamenVsKsql.csv' u 0:1 w l title "KSQL" lt 1,\
     'RamenVsKsql.csv' u 0:4 w l title "Ramen" lt 2

set output "RamenVsKsql_ram.svg"
set title "Free memory"
plot 'RamenVsKsql.csv' u 0:2 w l title "KSQL" lt 1,\
     'RamenVsKsql.csv' u 0:5 w l title "Ramen" lt 2

set output "RamenVsKsql_io.svg"
set title "Cumulative HD sectors written"
plot 'RamenVsKsql.csv' u 0:3 w l title "KSQL" lt 1,\
     'RamenVsKsql.csv' u 0:6 w l title "Ramen" lt 2
