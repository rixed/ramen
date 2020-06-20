module N = RamenName

let supervisor = N.service "supervisor"
let httpd = N.service "httpd"
let tunneld = N.service "tunneld"
let confserver = N.service "confserver"
let alerter = N.service "alerter"
let choreographer = N.service "choreographer"
let replayer = N.service "replayer"
let gc = N.service "gc"
let archivist = N.service "archivist"
let precompserver = N.service "precompserver"
let execompserver = N.service "execompserver"
let start = N.service "start"
