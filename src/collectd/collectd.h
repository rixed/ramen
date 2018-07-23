#ifndef COLLECTD_H_170919
#define COLLECTD_H_170919

/* Given a buffer with a collectd binary messages (one or several
 * concatenated makes no difference), return metrics contained
 * therein, as pairs of a string label and a double value (and
 * optionaly, but likely, a host and timestamp).
 *
 * Every data is allocated in the user provided memory block.
 * Some char pointer might point toward the received buffer (thus
 * the constness).
 *
 * In all error cases some metrics might have been decoded regardless,
 * and the metrics and num_metrics output parameters are always defined.
 */

enum collectd_decode_status {
  // When everything went well
  COLLECTD_OK,
  // When we were expecting more data at the end of the message
  COLLECTD_SHORT_DATA,
  // When we were not given enough memory
  COLLECTD_NOT_ENOUGH_RAM,
  // When we failed to make sense of the data
  COLLECTD_PARSE_ERROR,
};

#define COLLECTD_NB_VALUES 5
struct collectd_metric {
  char const *host; // If seen, or NULL.
  double time;  // In seconds since Unix epoch. 0 if unseen.
  char const *plugin_name;  // For instance: "interface".
  char const *plugin_instance;  // For instance: "wlp2s0".
  char const *type_name;  // For instance: "if_dropped".
  char const *type_instance;  // for instance: "1".
  // Up to 5 values (the worse case in a default install, for mysql_qcache).
  unsigned num_values;
  double values[COLLECTD_NB_VALUES];
};

extern enum collectd_decode_status collectd_decode(
  // The incoming message(s):
  size_t msg_size, char const *msg,
  // Where to draw data from:
  size_t mem_size, void *mem,
  // Output parameters: metric pairs and their number
  unsigned *num_metrics, struct collectd_metric **metrics);

#endif
