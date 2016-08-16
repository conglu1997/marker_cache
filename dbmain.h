#ifndef DBMAIN_INCLUDED
#define DBMAIN_INCLUDED

/**INC+***********************************************************************/
/* Header:    dbmain.h                                                       */
/*                                                                           */
/* Purpose:   Static class references.                                       */
/*                                                                           */
/* (C) COPYRIGHT DATA CONNECTION LIMITED                                     */
/*                                                                           */
/* $Id:: dbmain.h 179649 2015-10-26 12:47:47Z ag                          $  */
/* $URL:: http://enfieldsvn/repos/metaswitch/trunk/sas/code/ced/dbapp/dbm#$  */
/*                                                                           */
/**INC-***********************************************************************/

extern string db_name;
extern string db_port;

// The port that dbapp listens on for connections from network elements
// using the VPED library to send SAS events.
extern int sas_port;
extern db_msg_q* free_msg_q;
extern db_msg_q* work_msg_q;
extern db_msg_q* analytics_msg_q;
extern db_manager* db_mgr;
extern db_analytics_manager* db_analytics_mgr;
extern db_trail_mgr* trail_mgr;
extern db_event_table* event_table;
extern db_stat_table* stat_table;
extern db_trail_table* trail_table;
extern db_trailgroup_table* tg_table;
extern db_trail_id_map_table* trail_map_table;
extern db_statistics_manager* stats_mgr;
extern marker_cache* db_marker_cache;

#ifdef LICENSING
extern pthread_mutex_t     dcm_hourly_reset_mutex;
extern int64               epoch_hour;
extern std::atomic<uint64> local_used_capacity_f_blocks;
extern std::atomic<uint64> total_used_capacity_f_blocks;
extern std::atomic<uint64> licensed_capacity_f_blocks;
extern std::atomic<uint64> licensed_grace;
#endif

// Minimum time, in minutes, to use for the database roll time.
const int MINIMUM_ROLL_TIME_MINS = 30;

// Maximum time, in minutes, to use for the database roll time.
const int MAXIMUM_ROLL_TIME_MINS = 300;

// Percentage by which to scale various constants related to memory allocation
extern int scale_percent;

// Whether to write correlating markers for federated SAS.  The default is
// true, but this causes more data to be written to disk (and hence reduces
// scale) compared to V8.0 SAS and so a config file override option is
// provided.
extern bool write_federation_correlators;

// Inverse proportion of messages that are marked as radioactive when they are
// received by dbapp.
extern int radioactive_ratio;

// File stream used to log tracing of radioactive messages
extern ofstream radioactive_logger;

// Desired amount of time between rolls of database subtables, in milliseconds.
// In practice, because we will force each subtable to roll on an integer
// multiple of this time since the last scheduled timed roll, but we won't
// roll if the table rolled normally less than half this time ago, it is just
// about possible for a table to not roll for 1.5 times this time. It would
// require a very oddly sporadic supply of that data type, though.
extern int max_subtable_roll_time;

// The length of the interval to use when counting incoming messages for the
// purposes of enforcing a maximum message rate.
extern int max_msg_rate_interval_s;

// Whether to write encrypted events to the database.  The default is true,
// but this causes more data to be writen to disk and so a config file override
// options is provided.
extern bool write_encrypted_events;

#endif
