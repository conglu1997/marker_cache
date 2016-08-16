/* dbmain.cpp */
/**MOD+***********************************************************************/
/* Module:    dbmain.cpp                                                     */
/*                                                                           */
/* Purpose:   Application entry point                                        */
/*                                                                           */
/* (C) COPYRIGHT DATA CONNECTION LIMITED 2007 - 2008                         */
/*                                                                           */
/* $Id:: dbmain.cpp 195946 2016-06-23 14:34:05Z gml                        $ */
/* $URL:: http://enfieldsvn/repos/metaswitch/trunk/sas/code/ced/dbapp/dbma#$ */
/*                                                                           */
/**MOD-***********************************************************************/

#define TRC_FILE "dbmain"
#include <dbinc.h>

// IAGTrace includes and defines.  Note that the dbapp.mak makefile will have
// configured us as either OS_LINUX or OS_SOLARIS.
#define TRC_PRODUCT TRC_ID_SAS_DBAPP
#define TRC_GROUP   TRC_COMP_SAS_DBAPP_DBAPP

#include <aiag.h>
#include <atrcapi.h>
#include <algorithm> // For the std::min & std::max functions.

string db_name;
string db_port;
int sas_port;
db_msg_q* work_msg_q;
db_msg_q* analytics_msg_q;
db_manager* db_mgr;
db_analytics_manager* db_analytics_mgr;
db_trail_mgr* trail_mgr;
db_event_table* event_table;
db_stat_table* stat_table;
db_trail_table* trail_table;
db_trailgroup_table* tg_table;
db_trail_id_map_table* trail_map_table;
int scale_percent;
bool write_federation_correlators;
int max_subtable_roll_time;
db_statistics_manager* stats_mgr;
int radioactive_ratio;
const char* RADIOACTIVE_LOG_LOCATION =
                      "/var/opt/MetaViewSAS/tview/log/dbapp_message_trace.log";
ofstream radioactive_logger(RADIOACTIVE_LOG_LOCATION,
                                               ios_base::out | ios_base::app);
pthread_mutex_t radioactive_logger_mutex;
marker_cache* db_marker_cache;

#ifdef LICENSING
bool write_encrypted_events;
#endif

#ifdef DB_TIME
int64 db_time[DB_TIME_MAX + 1];
#endif

#ifdef LICENSING
pthread_t police_dog_thread_id;
extern "C" void* db_police_dog_thread_wrapper(void *context);
#endif

pthread_t sock_listen_thread_id;
extern "C" void* db_listener_thread_wrapper(void* context);

int main (int argc, char *argv[])
{
  // Parse the options
  char option_char;
  int rc;
  int cfg_subtable_roll_time;

#ifdef LICENSING
  MetaLic_HL_Init();
  db_license db_lic;
#endif

  TRC_BEGIN_FN("main");
  TRC_NRM(("Starting DBApp"));

  // Ignore SIGPIPE. It's far more straightforward to deal with errors during
  // write()s inline when they happen.
  signal(SIGPIPE, SIG_IGN);

  // The port that dbapp listens for VPED library connections on.
  sas_port = 6761;

  db_name = "dbtest_development";
  db_port = "5433";

  while (((--argc) > 0) &&
         ((*(++argv))[0] == '-'))
  {
    option_char = *(++(argv[0]));

    switch (option_char)
    {
      case 'p':
        sas_port = (unsigned int)atoi((++argv)[0]);
        --argc;
        if (sas_port == 0)
        {
          TRC_ERR(("Invalid port specified - exiting"));
          printf("invalid port\n");
          exit(RC_ERROR);
        }
        break;

      default:
        TRC_ERR(("Invalid option %c specified - exiting", option_char));
        printf("invalid option (%c)\n", option_char);
        printf("Usage: <-p port> <-v (verbose)>\n");
        exit(RC_ERROR);
        break;
    }
  }

  if (argc > 0)
  {
    db_name = string(argv[0]);
  }

  openlog("DbApp", LOG_PID, LOG_DAEMON);
  setlogmask(LOG_UPTO(LOG_NOTICE));

#ifdef LICENSING
  //---------------------------------------------------------------------------
  // Check we are licensed to start dbapp, and retrieve capacity information
  // from the license.
  //---------------------------------------------------------------------------
  if (!db_lic.file_exists())
  {
    // No license file.  Report the error back to Craft.
    TRC_NRM_SYSLOG(
         "This Service Assurance Server is unlicensed and cannot be started.");

    db_police_dog::db_notify_license_state(&db_lic,
                                           (NBB_CHAR*)"",
                                           (NBB_CHAR*)"",
                                           0,
                                           0,
                                           0,
                                           0);
    db_police_dog::db_notify_craft(FALSE);
    exit(RC_UNLICENSED);
  }

  if (!db_lic.file_valid())
  {
    //-------------------------------------------------------------------------
    // Failed to read or decode the license file.  Report the same "unlicensed"
    // error rather than revealing any further information.
    //
    // [We don't need to give any clues as to the exact problem with the
    // license key (be it bad-base64, bad-signature, or bad-tlv encoding)
    // because this is a value which should be pasted exactly and if it were
    // genuine it would "just work" (TM).]
    //-------------------------------------------------------------------------
    TRC_NRM_SYSLOG(
 "Service Assurance Server cannot be started because the license is invalid.");
    db_police_dog::db_notify_license_state(&db_lic,
                                           (NBB_CHAR*)"",
                                           (NBB_CHAR*)"",
                                           0,
                                           0,
                                           0,
                                           0);
    db_police_dog::db_notify_craft(FALSE);
    exit(RC_UNLICENSED);
  }

  //---------------------------------------------------------------------------
  // Store the ATCA chassis ID, if there is one.
  //---------------------------------------------------------------------------
  db_police_dog::db_store_chassis_id();

  //---------------------------------------------------------------------------
  // We check to see whether we have a ATCA chassis ID and then rely on dbapp
  // refusing to start if we're not on matching hardware. We only need to check
  // this at start of day as our hardware cannot subsequently change.  We do
  // not encrypt event data on ATCA systems.
  //---------------------------------------------------------------------------
  if (db_police_dog::db_is_atca())
  {
    //-------------------------------------------------------------------------
    // Set a flag to indicate that plain text events should be used.
    //-------------------------------------------------------------------------
    write_encrypted_events = false;

    // Produce a log so that it is easy to verify if this option has been
    // enabled.  This is used as part of the MOP process, so should not be
    // modified.
    TRC_NRM_SYSLOG("SAS dbapp event encryption: disabled.");
  }
  else
  {
    //-------------------------------------------------------------------------
    // Default to encrypting events.
    //-------------------------------------------------------------------------
    write_encrypted_events = true;

    // Produce a log so that it is easy to verify if this option has been
    // enabled.  This is used as part of the MOP process, so should not be
    // modified.
    TRC_NRM_SYSLOG("SAS dbapp event encryption: enabled.");
  }

  //---------------------------------------------------------------------------
  // We will do our full set of validity checks on the license within the
  // policedog thread...
  //---------------------------------------------------------------------------
#endif

  printf("Using database %s, listening on port %d\n", db_name.c_str(), sas_port);
  TRC_NRM_SYSLOG("SAS dbapp starting using database %s, listening on port %d",
                 db_name.c_str(), sas_port);
#if (TRC_COMPILE_LEVEL < TRC_LEVEL_DIS)
  TRC_NRM_SYSLOG("SAS dbapp is running with trace level %d and above compiled "
                 "in. Find trace in /var/opt/MetaViewSAS/tview/log/",
                 TRC_COMPILE_LEVEL);
#else
  TRC_NRM_SYSLOG("SAS dbapp is running without tracing compiled in. "
                 "Find logs in /var/opt/MetaViewSAS/tview/log/");
#endif

  db_manager::setup_perf_parameters();

  int nw_buf_size = db_manager::message_queue_size * DEFAULT_MSG_LENGTH;

  if (db_util::getConfigInt(
                          db_util::CONFIG_DISCARD_FEDERATION_CORRELATORS) != 0)
  {
    TRC_ALT_SYSLOG("WARNING:  Discarding federation correlating markers - "
                   "this server should not be used as part of a federation");
    write_federation_correlators = false;
  }
  else
  {
    TRC_NRM_SYSLOG("Writing federation correlating markers");
    write_federation_correlators = true;
  }

  // Get the config setting to determine what ratio of messages will be marked
  // as radioactive.
  if (db_util::getConfigInt(db_util::CONFIG_DB_TRACE_MESSAGES) == 0)
  {
    // Tracing radioactive messages is turned off
    radioactive_ratio = 0;
    TRC_NRM_SYSLOG("Not tracing any messages through dbapp");
  }
  else
  {
    radioactive_ratio =
                   db_util::getConfigInt(db_util::CONFIG_DB_RADIOACTIVE_RATIO);

    if (radioactive_ratio == 0)
    {
      if (scale_percent >= 100)
      {
        // Stand-alone system, set radioactive_ratio to 500,000
        radioactive_ratio = 500000;
      }
      else
      {
        // Co-resident system, set radioactive_ratio to 10,000
        radioactive_ratio = 10000;
      }
    }

    pthread_mutex_init(&radioactive_logger_mutex, NULL);

    if (radioactive_logger.fail())
    {
      TRC_ALT_SYSLOG("Failed to open %s for tracing message progress "
                     "through dbapp.", RADIOACTIVE_LOG_LOCATION);
    }
    else
    {
      TRC_NRM_SYSLOG("Tracing approximately 1 in %d messages through dbapp. "
                     "Tracing can be found in %s",
                     radioactive_ratio, RADIOACTIVE_LOG_LOCATION);
    }
  }

  // Read the maximum subtable roll time from config.
  cfg_subtable_roll_time = db_util::getConfigInt(db_util::CONFIG_DB_ROLL_TIME);

  if (cfg_subtable_roll_time == 0)
  {
    // As no roll time has been specified, determine the roll time based upon
    // the maximum event age configuration. If this is set very high (e.g.
    // over four weeks) then a higher roll time is required, otherwise there
    // will be too many sub-tables. The only downside of a longer roll time
    // may be that searches can take slightly longer.
    //
    // The roll time is calculated using a simple algorithm which basically
    // uses 30 minutes roll time per week of logs required.
    TRC_NRM_ALWAYS(("Subtable roll time absent. Determine value to use."));
    int cfg_max_event_age_in_days =
               db_util::getConfigInt(db_util::CONFIG_DB_MAX_EVENT_AGE_IN_DAYS);

    TRC_NRM_ALWAYS(("Max age configured as %d days.",
                   cfg_max_event_age_in_days));

    // Divide configured age by seven to determine number of weeks. Multiply
    // this by 30 to get the roll time. There is a minimum roll time so call
    // through to std::max to ensure the roll time is at least the minimum.
    // Finally, compare this to the maximum allowed roll time, using
    // std::min, to ensure that it is not exceeded.
    cfg_subtable_roll_time =
       std::min(MAXIMUM_ROLL_TIME_MINS,
                std::max(MINIMUM_ROLL_TIME_MINS,
                         ((cfg_max_event_age_in_days / 7) * 30)));
  }

  TRC_NRM_ALWAYS(("Subtable roll time configured to %d minutes.",
                  cfg_subtable_roll_time));

  // Config value is in minutes, and we want it in milliseconds.
  max_subtable_roll_time = cfg_subtable_roll_time * 60 * 1000;

  // Create the work and analytics message queues.
  work_msg_q = new db_msg_q(nw_buf_size);

  // Init the connection to SAS if configured.
  string sas_address = db_util::getConfigString(db_util::CONFIG_SAS_ADDRESS);

  if (!sas_address.empty())
  {
#ifdef OS_LINUX
    TRC_NRM_ALWAYS(("Init connection to SAS %s", sas_address.c_str()));
    char hostname[MAXHOSTNAMELEN];
    gethostname(hostname, sizeof(hostname));

    int rc = SAS::init(hostname,
                       "MetaView SAS",
                       "com.metaswitch.sas",
                       sas_address,
                       &SAS::discard_logs);

    if (rc != SAS_INIT_RC_OK)
    {
      TRC_ERR_ALWAYS(("Failed to init connection to SAS %s",
                      sas_address.c_str()));
    }
#else
    TRC_ERR_ALWAYS(("No support for SAS logging on Solaris"));
#endif
  }

  // Create the database manager.
  db_mgr = new db_manager();
  db_mgr->init();

  // Create the analytics manager.
  db_analytics_mgr = new db_analytics_manager();
  db_analytics_mgr->init();

#ifdef LICENSING
  // First initialize the licensed capacity.
  // This is also by the watchdog thread, but the watchdog thread can take a
  // while to warm up, so we initialize it here as well, to guarantee that the
  // capacity is ready for dbmanager to use.
  licensed_capacity_f_blocks.store(db_lic.get_standalone_capacity());
  licensed_grace.store(0);

  //---------------------------------------------------------------------------
  // Now that we have created the database manager, we can start our policedog
  // thread to communicate with the dongle and verify that the license file
  // matches the token group and slot id etc.
  //
  // This thread must handle the upstart contract to let Craft know whether we
  // are (at least initially) licensed or not.
  //---------------------------------------------------------------------------
  rc = pthread_create(&(police_dog_thread_id),
                      NULL,
                      db_police_dog_thread_wrapper,
                      db_mgr);

  if (rc == 0)
  {
    TRC_NRM(("Created police dog thread %u", police_dog_thread_id));
    rc = pthread_detach(police_dog_thread_id);
  }
  else
  {
    TRC_ERR_SYSLOG("Failed to create thread (error %d)", rc);
  }

  //---------------------------------------------------------------------------
  // Having successfully started our police dog thread to repeatedly check our
  // licensing and process security, we now continue until we hear otherwise.
  //
  // Note that we will become the DB Manager below and from then we will expect
  // regular contact back from the police dog thread.  If that doesn't happen,
  // we'll assume the worst and terminate.
  //---------------------------------------------------------------------------
#endif

  // Create the socket listener thread.
  rc = pthread_create(&(sock_listen_thread_id),
                      NULL,
                      db_listener_thread_wrapper,
                      NULL);

  if (rc == 0)
  {
    TRC_NRM(("Created socket listener thread %u", sock_listen_thread_id));
    rc = pthread_detach(sock_listen_thread_id);

    // Now pass control to the database manager.
    db_mgr->process_msgs();
  }
  else
  {
    TRC_ERR_SYSLOG("Failed to create dbapp socket listener thread (error %d) - terminating", rc);
  }

  TRC_END_FN();
  return rc;
}
