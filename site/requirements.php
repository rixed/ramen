<? include 'header.php' ?>
<h1>Initial requirements</h1>

<h2>Delivery Guarantees and Scale</h2>

<requirement>
  <req>
    <p>The system is designed to run on a single server so the woes of networking have not been designed around. Beside, the incoming flow is made of many small events, the individual contribution of each on the final outcome is assumed to be negligible. In case of overloading we want back-pressure to be applied and incoming messages being delayed/rejected but not lost once accepted.</p>
  </req>
  <decision>
    <p>Single or few servers only.</p>
  </decision>
</requirement>

<h2>Simplicity</h2>

<requirement>
  <req>
    <p>Ramen should be programmable through a data manipulation language as declarative (as opposed to procedural) and as familiar as possible.</p>
  </req>
  <decision>
    <p>It was initially considered that the best trade of between simplicity and efficiency would be to use an actual programming language with a syntax and a library of functions tailored toward stream processing (as <a href="http://riemann.io">Riemann</a> does for instance with Clojure), but the prototype has proven too limited: First, speed would have to be sacrificed (regardless of what language we would use to embed Ramen in), and then it was constraining how we could distribute processing amongst several processes or servers.</p>
    <p>Eventually it was decided to implement a SQL like language that's less demanding from users, more flexible to our ever changing requirements and that Ramen is free to compile into any combination of programs/threads/functions as is deemed desirable.</p>
  </decision>
</requirement>

<h2>Performances</h2>

<requirement>
  <req>
    <p>The system must be able to handle about 500k ops/sec/server.</p>
  </req>
  <decision>
    <ul>
      <li>The operations must be compiled down to machine code rather than being interpreted;</li>
      <li>Operations must run in parallel in different threads/processes;</li>
      <li>Event transmission along the stream must be as direct as possible; in particular, there must be no central message broker;</li>
      <li>It is not possible to do only direct function calls due to the fact that different operations will run on different threads most of the time;</li>
      <li>(de)serialization of events must not be required;</li>
      <li>In case of overcapacity back-pressure should slows down the input flow rather than lead to loss of events.</li>
    </ul>
    <p>Therefore we need some kind of per operation lock-less input queue in shared memory; Ramen uses ring-buffers.</p>
  </decision>
  <status>
    <p>Currently Ramen generates native code via the OCaml compiler, therefore the generated code uses garbage collection and uses boxed values that need to be serialized/deserialized out of the ringbuffers.</p>
    <p>The ringbuffers should be dynamically sized but are still constant sized at the moment.</p>
    <p>Direct function calls are not supported yet as a message passing mechanism.</p>
    <p>Support for more than one machine is not supported yet.</p>
    <p>All of the above to be addressed in the future.</p>
  </status>
</requirement>

<h2>Versatility</h2>

<requirement>
  <req>
    <p>Although focusing on monitoring and alerting, as little constraints as possible should be imposed on the input stream. In particular, incoming events can describe anything, and might have undergone some aggregation already, and come with several time stamps attached.</p>
  </req>
  <decision>
    <ul>
      <li>Events can be of any type reachable from the base types and the compound types;</li>
      <li>Events can have a start and end time, which are both taken into account when extracting time series;</li>
      <li>How to build these event-time from actual event is part of the schema so incur no cost;</li>
    </ul>
  </decision>
  <status>
    <p>Ramen supports tuples, arrays and lists as compound types but is still missing records (basically just tuples with syntactic sugar for named fields).  Also, events are constrained to be tuples but that will be alleviated at a later stage.</p>
  </status>
</requirement>

<h2>Remembering past values</h2>

<requirement>
  <req>
    <p>We also want to be able to use Ramen for troubleshooting/capacity planning/etc so it must be able to answer possibly new queries on past data.</p>
  </req>
  <decision>
    <ul>
      <li>Given the total storage space available and a desired retention of some key stages in the stream processing, Ramen allocates the storage space in order to optimize the processing of future queries;</li>
      <li>Every new query can be run either on the live stream of data or on a past time range or both on the recent history and then on live data;</li>
      <li>Data is stored either in uncompressed form (incurs little additional processing cost) or in compressed form (ORC file);</li>
      <li>Any new query branched off the query tree after a save point can be sent archived data;</li>
    </ul>
  </decision>
  <status>
    <p>Transitioning from past to live is yet to be implemented. Also ORC support is still to be done.</p>
  </decision>
</requirement>

<h2>Batteries included</h2>

<requirement>
  <req>
    <p>Ramen should come with all the necessary components required to build a small monitoring solution.</p>
  </req>
  <decision>
    <ul>
      <li>Ramen should accept mere CSV files as input, in addition to fancier interfaces.</li>
      <li>For dashboarding, it should be easy to use Grafana by implementing the Graphite API.</li>
      <li>Ramen should be able to connect to an external alert management system such as the alertmanager of Prometheus, but must also be able to perform "the last mile" of alert delivery out of the box for simplicity.</li>
    </ul>
  </decision>
  <status>
    <p>The only possible ways to inject data at the moment are CSV files, collectd or netflow protocols, and direct ringbuffer writes. More obviously need to be implemented, popular message queue being on top of the list.</p>
  </status>
</requirement>

<? include 'footer.php' ?>
