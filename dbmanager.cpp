/* dbmanager.cpp */
/**MOD+***********************************************************************/
/* Module:    dbmanager.cpp                                                  */
/*                                                                           */
/* Purpose:   Database manager                                               */
/*                                                                           */
/* (C) COPYRIGHT DATA CONNECTION LIMITED 2007 - 2008                         */
/*                                                                           */
/* $Id:: dbmanager.cpp 194914 2016-06-09 09:27:02Z md3                     $ */
/* $URL:: http://enfieldsvn/repos/metaswitch/trunk/sas/code/ced/dbapp/dbma#$ */
/*                                                                           */
/**MOD-***********************************************************************/

#define TRC_FILE "dbmanager"
#include <dbinc.h>

int db_manager::NUMBER_OF_MARKER_TYPES;

void db_manager::init()
{
  PGresult *res = NULL;
  char query[256];

  TRC_BEGIN_FN("init");

  // Initialise system map mutex.
  pthread_mutex_init(&_system_map_mutex, NULL);

  // Connect to the database.
  db_connect();

  // Create the statistics manager.
  stats_mgr = new db_statistics_manager();

  // We store 1 set of statistics for each DB_STATS_PERIOD
  _internal_statistics.reset();

  // Get the IDs of events to be ignored from the config file
  vector<int32> ignore_events = db_util::getConfigInts(db_util::CONFIG_DB_IGNORE_EVENTS);
  _ignore_events_len = ignore_events.size();
  _ignore_events = new int32[_ignore_events_len];

  for (int ii = 0; ii < _ignore_events_len; ii++)
  {
    TRC_NRM_SYSLOG("Database configured to ignore events with ID 0x%08X", ignore_events[ii]);
    _ignore_events[ii] = ignore_events[ii];
  }

  // Get the IDs of markers to be ignored from the config file
  vector<int32> dont_write_markers = db_util::getConfigInts(db_util::CONFIG_DB_DONT_WRITE_MARKERS);
  _dont_write_markers_len = dont_write_markers.size();
  _dont_write_markers = new int32[_dont_write_markers_len];

  for (int ii = 0; ii < _dont_write_markers_len; ii++)
  {
    TRC_NRM_ALWAYS(("Database configured to not write markers with ID 0x%08X", dont_write_markers[ii]));
    _dont_write_markers[ii] = dont_write_markers[ii];
  }

  // Get the number of copy threads.
  int num_copy_threads = db_util::getConfigInt(db_util::CONFIG_NUM_COPY_THREADS);

  if (num_copy_threads == 0)
  {
    TRC_NRM_SYSLOG("Number of copy threads config absent, defaulting to 2");
    num_copy_threads = 2;
  }
  else
  {
    TRC_NRM_SYSLOG("Using %d copy threads", num_copy_threads);
  }

  // Create the trail manager.
  TRC_DBG(("Create trail manager"));
  trail_mgr = new db_trail_mgr(tg_cache_size(), trail_cache_size());

  // Create the marker cache
  TRC_DBG(("Create marker cache"));
  db_marker_cache = new marker_cache(6000000000); // 6GB test on virtual machine
  TRC_DBG(("Create bloom filter"));
  db_marker_cache->create(1, 0.001, 3000000000); // 3 billion

  // Create the trail, trail group and event tables
  TRC_DBG(("Create trail and event tables"));
  trail_table = new db_trail_table(trail_table_buf_size());
  tg_table = new db_trailgroup_table(tg_table_buf_size());
  event_table = new db_event_table(event_table_buf_size());
  stat_table = new db_stat_table(event_table_buf_size());
  trail_map_table = new db_trail_id_map_table(trail_map_table_size());

  // Create all the marker tables within the map for mapping marker_id to
  // marker table.
  init_marker_table_managers();

  // Initialise all the tables.
  TRC_NRM(("Initialise all tables"));
  trail_table->init();
  tg_table->init();

  // Trail ID mappings table must be initialised before the event tables as
  // the event tables may write to the trail ID mappings table during start up.
  trail_map_table->init();
  event_table->init();
  stat_table->init();

  for (map<int,db_marker_table*>::iterator i = _marker_map.begin();
       i != _marker_map.end();
       i++)
  {
    (i->second)->init();
  }

  TRC_DBG(("Create copy threads and index thread"));

  // Create the threads that copy data to the database.  This must be done
  // after all the tables have been initialised.
  db_table::create_copy_threads(num_copy_threads);

  // Create the background thread for creating indexes.
  db_table::create_index_thread();

  // Read the database to initialise the trail group identifier and branch
  // identifiers, to make sure no new objects clash with any in the database.
  //
  // This must be done after the trail table has been initialised, as this
  // updates the min/max trail_gids for any subtables not closed when dbapp
  // was last stopped.
  trail_mgr->initialise_ids();

  // As we are just starting DBapp, there can't be any connected systems.  The
  // database state could indicate that there are, if for example DBapp has
  // crashed while one or more systems were connected.
  //
  // Reset the systems table by setting the time_disconnected field for all
  // systems which look like they are connected, i.e. those with a non null
  // time_connected row and a null time_disconnected.
  sprintf(query,
          "UPDATE systems SET time_disconnected=current_timestamp "
          "WHERE time_connected IS NOT NULL AND time_disconnected IS NULL;");
  res = PQexec(db_conn(), query);

  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    TRC_ERR_SYSLOG("PQexec(update system) failed, "
                   "status = %d\n  error: %s\n  %s",
                   PQresultStatus(res),
                   PQresultErrorMessage(res),
                   query);
  }

#ifdef LICENSING
  epoch_hour = db_util::getHoursSinceEpoch();
  pthread_mutex_init(&dcm_hourly_reset_mutex, NULL);
#endif

  TRC_END_FN();
}

void db_manager::init_marker_table_managers()
{
  TRC_BEGIN_FN("init_marker_table_managers");
  TRC_NRM(("Create marker tables"));

  int buf_size = marker_table_buf_size();

  // Consolidated DN tables are keyed under MARKER_ID_CALLING_DN.
  _marker_map[MARKER_ID_CALLING_DN]           = new db_dn_table(buf_size);

  // non-DN markers:
  _marker_map[MARKER_ID_ADDITIONAL_NUM]       = new db_additionalnums_table(buf_size);
  _marker_map[MARKER_ID_END_TIME]             = new db_endtime_table(buf_size);
  _marker_map[MARKER_ID_GR303_LINE]           = new db_gr303line_table(buf_size);
  _marker_map[MARKER_ID_SUBG_LINE]            = new db_subgline_table(buf_size);
  _marker_map[MARKER_ID_BLES_LINE]            = new db_blesline_table(buf_size);
  _marker_map[MARKER_ID_ISDN_PRI]             = new db_isdnpri_table(buf_size);
  _marker_map[MARKER_ID_ISUP_TRUNK]           = new db_isuptrunk_table(buf_size);
  _marker_map[MARKER_ID_MF_TRUNK]             = new db_mftrunk_table(buf_size);
  _marker_map[MARKER_ID_SIP_LINE]             = new db_sipline_table(buf_size);
  _marker_map[MARKER_ID_SIP_BINDING]          = new db_sipbinding_table(buf_size);
  _marker_map[MARKER_ID_SIP_TRUNK]            = new db_siptrunk_table(buf_size);
  _marker_map[MARKER_ID_DS0]                  = new db_ds0_table(buf_size);
  _marker_map[MARKER_ID_SIP_CALL_ID]          = new db_sipcallid_table(buf_size);
  _marker_map[MARKER_ID_PROT_ERROR]           = new db_protocolerror_table(buf_size);
  _marker_map[MARKER_ID_PHONE_MAC]            = new db_phonemac_table(buf_size);
  _marker_map[MARKER_ID_CALL_INFO]            = new db_callinfo_table(buf_size);
  _marker_map[MARKER_ID_TEST_QUERY]           = new db_testqueries_table(buf_size);
  _marker_map[MARKER_ID_MWI_TARGET_DN]        = new db_mwitargetdns_table(buf_size);
  _marker_map[MARKER_ID_LSM_MONITORING_DN]    = new db_lsm_monitoring_dns_table(buf_size);
  _marker_map[MARKER_ID_LSM_MONITORED_DN]     = new db_lsm_monitored_dns_table(buf_size);
  _marker_map[MARKER_ID_OUTBOUND_CALLING_URI] = new db_outbound_calling_uri_table(buf_size);
  _marker_map[MARKER_ID_INBOUND_CALLING_URI]  = new db_inbound_calling_uri_table(buf_size);
  _marker_map[MARKER_ID_OUTBOUND_CALLED_URI]  = new db_outbound_called_uri_table(buf_size);
  _marker_map[MARKER_ID_INBOUND_CALLED_URI]   = new db_inbound_called_uri_table(buf_size);
  _marker_map[MARKER_ID_REMOTE_IP]            = new db_remote_ip_table(buf_size);
  _marker_map[MARKER_ID_SIP_ALL_REGISTER]     = new db_sip_all_register_table(buf_size);
  _marker_map[MARKER_ID_SIP_SUBSCRIBE_NOTIFY] = new db_sip_subscribe_notify_table(buf_size);
  _marker_map[MARKER_ID_CFE_DN]               = new db_cfe_dns_table(buf_size);
  _marker_map[MARKER_ID_MVD_MOVABLE_BLOCK]    = new db_movable_block_table(buf_size);
  _marker_map[MARKER_ID_BROADBAND_TEST]       = new db_broadband_test_table(buf_size);

  // Only add the correlating marker table to the marker map once as this
  // avoids other bits of dbapp doing things to it multiple times (e.g.
  // initializing it).
  _marker_map[MARKER_ID_MG_CORRELATOR]        = new db_correlating_marker_table(buf_size);
  _marker_map[MARKER_ID_V5_LINE]              = new db_v5line_table(buf_size);
  db_manager::NUMBER_OF_MARKER_TYPES = _marker_map.size();

  TRC_END_FN();
}

PGresult* db_manager::executeQuery(PGconn* db_conn, const int tmr, string* pSQL)
{
  const char* sql_c_str = pSQL->c_str();
  PGresult* res = NULL;

  TRC_BEGIN_FN("executeQuery");

  TRC_DBG(("Executing query: %s", sql_c_str));

  START_TIMER(*pSQL, 100, &db_time[tmr]);
  res = PQexec(db_conn, sql_c_str);
  END_TIMER(*pSQL);

  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    TRC_ERR_SYSLOG("Failed to insert data into statistics database, "
                   "status = %d\n  %s\n%s",
                   PQresultStatus(res),
                   PQresultErrorMessage(res),
                   sql_c_str);
  }

  TRC_END_FN();

  return res;
}

db_marker_table* db_manager::marker_table(int32 marker_id)
{
  db_marker_table *marker;
  TRC_BEGIN_FN("marker_table");
  TRC_DBG(("getting marker table for marker id %08X", marker_id));

  // Federation correlating markers are keyed under the MARKER_ID_MG_CORRELATOR
  // marker ID.
  if (IS_FEDERATION_CORRELATING_MARKER(marker_id))
  {
    TRC_DBG(("This is a federation correlating marker"));
    marker_id = MARKER_ID_MG_CORRELATOR;
  }
  // Consolidated DN markers are keyed under the MARKER_ID_CALLING_DN marker
  // ID.
  else if (IS_CONSOLIDATED_DN_MARKER(marker_id))
  {
    TRC_DBG(("This is a consolidated DN marker"));
    marker_id = MARKER_ID_CALLING_DN;
  }

  map<int,db_marker_table*>::iterator i = _marker_map.find(marker_id);
  if (i == _marker_map.end())
  {
    TRC_ERR_SYSLOG("marker identifier 0x%08X unknown in DB", marker_id);
    marker = NULL;
  }
  else
  {
    marker = (i->second);
  }

  TRC_END_FN();
  return marker;
}

// Update a trail group with a new branch/trail group ID.  Set the branch ID
// to 0 to update the trail group ID only.  TG init time is only required for
// updating the TG ID.
//
// Metadata tables (e.g. calling_dn_tables) are updated immediately, but
// modifications to the marker and trails tables are queued until the next
// dbtable flush for that table.
void db_manager::do_updates(int64 old_tg_id,
                            int64 old_branch_id,
                            int64 new_tg_id,
                            int64 new_branch_id,
                            int64 new_tg_init_time)
{
  TRC_BEGIN_FN("do_updates");
  TRC_NRM(("Performing update: tg_id=%vd, branch_id=%vd => %vd, %vd",
           old_tg_id, old_branch_id, new_tg_id, new_branch_id));

  _internal_statistics._num_updates++;
  TRC_DBG(("Number of updates is now %d", _internal_statistics._num_updates));

  // Queue the update on the trail table.
  TRC_DBG(("Queue update for trails table"));
  trail_table->update(old_tg_id,
                      old_branch_id,
                      new_tg_id,
                      new_branch_id,
                      new_tg_init_time);

  // Queue updates for all the marker tables.
  for (map<int,db_marker_table*>::iterator i = _marker_map.begin();
       i != _marker_map.end();
       i++)
  {
    TRC_DBG(("Queue update for table %s", (i->second)->getName().c_str()));
    (i->second)->update(old_tg_id,
                        old_branch_id,
                        new_tg_id,
                        new_branch_id,
                        new_tg_init_time);
  }

  TRC_END_FN();
}

// Establish a connection to the database.
//
// The connection uses the default (system) time zone, rather than UTC.
// UTC would be preferable, except that dbapp originally used the default
// timezone, so any change would affect how Postgres creates timestamps (e.g.
// the time field in marker xxx_tables tables, which defaults to now()).
//
// Most database operations don't care about the time zone for the session.
// Timestamps that are read and written as a string don't need time zone
// conversion.  However in a few places dbapp extracts the time as seconds
// since the epoch, which is affected.  These queries have to explicitly
// request the time in UTC.
//
// (see http://www.postgresql.org/docs/8.1/interactive/sql-set.html)
void db_manager::db_connect()
{
  TRC_BEGIN_FN("db_connect");

  // Connect to the database.
  TRC_NRM_SYSLOG("Connecting to database %s", db_name.c_str());
  _db_conn =
           PQconnectdb(("dbname = " + db_name + " port = " + db_port).c_str());

  if (PQstatus(_db_conn) != CONNECTION_OK)
  {
    TRC_ERR_SYSLOG("Failed to open database %s, status = %d",
                   db_name.c_str(),
                   PQstatus(_db_conn));
    PQfinish(_db_conn);
    TRC_ERR_SYSLOG(("dbapp terminating"));
    exit(RC_ERROR);
  }

  TRC_NRM_SYSLOG("Database %s opened", db_name.c_str());
  TRC_END_FN();
}

bool db_manager::ignore_msg(db_msg* msg)
{
  bool ret = false;
  TRC_BEGIN_FN("ignore_msg");

  char * hdr = msg->msg_data;

  if (GET_MSG_HDR_TYPE(hdr) == MSG_HEARTBEAT)
  {
    TRC_DBG(("Ignoring heartbeat message from system %d", msg->system_id));
    return true;
  }

  if (_ignore_events_len == 0)
  {
    TRC_DBG(("Not configured to ignore any events"));
    return false;
  }

  if ((IS_VALID_MSG_VERSION(hdr)) && (GET_MSG_HDR_TYPE(hdr) == MSG_EVENT))
  {
    int32 event_id = db_util::get_int32(GET_EVENT_MSG_EVENT_ID(msg->msg_data));
    ret = this->is_event_id_in_ignore_list(event_id);
  }

  TRC_END_FN();
  return ret;
}

bool db_manager::is_event_id_in_ignore_list(int32 event_id)
{
  bool ret = false;
  TRC_BEGIN_FN("is_event_id_in_ignore_list");

  // Check the event ID against the list of event IDs to ignore.
  for (int i = 0; i < _ignore_events_len; i++)
  {
    if (event_id == _ignore_events[i])
    {
      TRC_DBG(("Ignore event with ID %d", event_id));
      ret = true;
      break;
    }
  }

  TRC_END_FN();
  return ret;
}

void db_manager::update_licensed_capacity(uint64* pDefCapacity,
                                          uint64* pTotal_bytes_this_hour,
                                          uint64  local_bytes_this_hour)
{
  TRC_BEGIN_FN("update_licensed_capacity");
#ifdef LICENSING

  // Read the (latest) number of capacity blocks cached from the license.
  uint64 new_capacity = 0;
  uint64 new_capacity_f_blocks = licensed_capacity_f_blocks.load();

  // Read the running total of f-blocks written in this hour across all SASes
  // in the deployment, but return the value in bytes to caller.
  uint64 total_f_blocks = total_used_capacity_f_blocks.load();
  *pTotal_bytes_this_hour = total_f_blocks * SIZE_OF_CAPACITY_BLOCK / 10000;

  // And update the locally written total so that the policedog can report the
  // new number.
  local_used_capacity_f_blocks.store(local_bytes_this_hour * 10000 /
                                                       SIZE_OF_CAPACITY_BLOCK);

  // Standalone capacity is measured in ten-thousandths of a capacity block.
  if (new_capacity_f_blocks == LIC_CAPACITY_UNLIMITED)
  {
    // Capacity set to "unlimited" which we define as 10,000 capacity blocks.
    new_capacity = SIZE_OF_CAPACITY_BLOCK * 10000;

    if (new_capacity != *pDefCapacity)
    {
      TRC_NRM_SYSLOG(
          "SAS is starting or has detected a change in the licensed capacity");
      TRC_NRM_SYSLOG(
        "This SAS is not capacity limited - setting allowed scale to maximum");
      *pDefCapacity = new_capacity;
    }
  }
  else
  {
    new_capacity = new_capacity_f_blocks * SIZE_OF_CAPACITY_BLOCK / 10000;
    if (new_capacity != *pDefCapacity)
    {
      if (*pDefCapacity > 0) // IF we are not starting up.
      {
        TRC_NRM_SYSLOG("SAS has detected a change in the licensed capacity");
        TRC_NRM_ALWAYS(("Previous licensed capacity was %ld", *pDefCapacity));
      }

      *pDefCapacity = new_capacity;
      TRC_NRM_ALWAYS(("This SAS is licensed to %ld bytes per hour.",
                                                     (long int) new_capacity));
    }
  }

#else
  // We still draw capacity graphs on Solaris despite the absence of licensing
  // so make sure our running total is in step with what we've done locally!
  *pDefCapacity = SIZE_OF_CAPACITY_BLOCK * 10000;
  *pTotal_bytes_this_hour = local_bytes_this_hour;
#endif

  TRC_END_FN();
}

void db_manager::maybe_commit_tgs(int64 timestamp)
{
  TRC_BEGIN_FN("maybe_commit_tgs");

  if ((timestamp - _last_tg_commit_timestamp) > trail_group_commit_period_ms)
  {
    TRC_DBG(("Trailgroup commit occuring on configurable timing window - %dms",
             trail_group_commit_period_ms));

    // We will commit tgs, so update the reference timestamp.
    _last_tg_commit_timestamp = timestamp;

    // See if any trail groups need to be flushed to the database.
    commit_tgs(timestamp);
  }

  TRC_END_FN();
}

void db_manager::maybe_flush_tables(bool idle, int64 timestamp)
{
  TRC_BEGIN_FN("maybe_flush_tables");

  bool flushed = false;

  // If the system indicates it is idle, then flush all tables, clearing
  // queues - otherwise, only flush those tables that have gone 5 seconds
  // without a flush.  This has the effect that during load, the tables won't
  // all be flushing in unison, but also won't be flushing unnecessarily often.
  // Update the reference timestamp when tables are flushed.
  if (idle)
  {
    flushed = true;
    flush_tables();
    _last_flush_table_timestamp = timestamp;
  }
  else if ((timestamp - _last_flush_table_timestamp) > table_flush_period_ms)
  {
    TRC_DBG(("Table flush occuring on configurable timing window - %dms",
             table_flush_period_ms));
    flushed = true;
    flush_tables(timestamp - table_flush_period_ms);
    _last_flush_table_timestamp = timestamp;
  }

  TRC_END_FN();
}

void db_manager::maybe_purge_systems(int64 timestamp)
{
  TRC_BEGIN_FN("maybe_purge_systems");

  if ((timestamp - _last_system_purge_timestamp) > SYSTEM_PURGE_DELAY)
  {
    TRC_NRM(("Purging system trails"));

    db_trail_cache::purge_disconnected();
    _last_system_purge_timestamp = timestamp;
  }

  TRC_END_FN();
}

void db_manager::maybe_do_statistics(uint64 &local_60th,
                                     uint64 &local_bytes_this_hour,
                                     uint64 &last_local_bytes_reported,
                                     uint64 &total_60th,
                                     uint64 &total_hwm,
                                     uint64 &last_total_bytes_reported,
                                     uint64 &total_bytes_this_hour,
                                     uint64 &local_discards,
                                     uint64 &def_licensed_bytes_per_hour,
                                     int64  &last_queue_depth,
                                     int64  &last_stat_timestamp,
                                     int64  &hours_since_epoch,
                                     int64  &timestamp,
                                     int    &msg_count)
{
  TRC_BEGIN_FN("maybe_do_statistics");

  if ((timestamp - _last_stats_check_timestamp) > stats_check_period_ms)
  {
    TRC_DBG(("Stats check occuring on configurable timing window - %dms",
            stats_check_period_ms));

    // Update reference timestamp.
    _last_stats_check_timestamp = timestamp;

    if (timestamp > (last_stat_timestamp + STATS_PERIOD))
    {
      // A minute has passed since we last collected stats.
      last_stat_timestamp += STATS_PERIOD;

      // Have we moved to a new hour as well?
      bool top_of_hour = false;

      if (hours_since_epoch != db_util::getHoursSinceEpoch())
      {
        // At the end of the hour we reset both our local and total
        // capacity byte counters.  (If we could guarantee connectivity to
        // the DCM then we would rely on it to reset the total value
        // automatically for us, but we need to be defensive on behalf of
        // honest customers in case they lose their connection and then
        // wrongly get denied SAS service.)
        TRC_NRM_ALWAYS(("End of hour.  Resetting Capacity counters"));

        // Prior to resetting, we note the difference between our high
        // water marks and the last values we reported so that we still
        // record an accurate 60th stat for the hour.
        top_of_hour = true;
        local_60th = local_bytes_this_hour - last_local_bytes_reported;
        total_60th = total_hwm - last_total_bytes_reported;

        // Now it's safe for us to reset the other values.
        local_bytes_this_hour = 0;
        total_bytes_this_hour = 0;
        last_local_bytes_reported = 0;
        last_total_bytes_reported = 0;
        total_hwm = 0;
        local_discards = 0;

        hours_since_epoch = db_util::getHoursSinceEpoch();

#ifdef LICENSING
        // We need to acquire the dcm hourly reset mutex before resetting
        // the counters upon which the policedog operates.  We advance the
        // epoch hour to alert the policedog to this reset.
        pthread_mutex_lock(&dcm_hourly_reset_mutex);
        epoch_hour = hours_since_epoch;
        local_used_capacity_f_blocks.store(0);
        total_used_capacity_f_blocks.store(0);
        pthread_mutex_unlock(&dcm_hourly_reset_mutex);
#endif
      }

      TRC_NRM(("Pass statistics to stats thread"));

      // Periodically pass message statistics to the statistics thread.
      int depth = work_msg_q->depth();
      _statistics._timestamp                 = timestamp;
      _statistics._max_queue_length          = work_msg_q->max_depth();
      _statistics._min_queue_length          = work_msg_q->min_depth();
      _statistics._max_queue_used_percentage = work_msg_q->max_size_used_percentage();
      _statistics._min_queue_used_percentage = work_msg_q->min_size_used_percentage();
      _statistics._num_discarded             = work_msg_q->overruns() +
                                               _internal_statistics._num_licensing_discards;
      _statistics._num_discarded_licensing   = _internal_statistics._num_licensing_discards;
      _statistics._num_received              = _internal_statistics._num_messages +
                                               work_msg_q->overruns() +
                                               depth -
                                               last_queue_depth;
      _statistics._num_processed             = _internal_statistics._num_messages;
      _statistics._num_calls                 = _internal_statistics._num_calls;
      _statistics._num_updates               = _internal_statistics._num_updates;
      _statistics._num_events                = _internal_statistics._num_events;
      _statistics._num_association_messages  = _internal_statistics._num_association_messages;
      _statistics._num_markers               = _internal_statistics._num_markers;
      _statistics._num_trails                = trail_map_table->_num_trails;
      _statistics._bytes_received            = work_msg_q->vped_bytes();

      // LICENSING STATS:
      //
      // - capacity_used_total is the total capacity used across the
      //   deployment (in discrete 1 minute chunks).
      //
      // - capacity_used_local is the capacity used locally by this SAS
      //   (also in 1 minute chunks).
      //
      if (top_of_hour)
      {
        // At the top of the hour we had to reset our counters, so now we
        // use the values we calculated above.
        _statistics._capacity_used_total =
                                    bytes_to_capacity_blocks(total_60th);
        _statistics._capacity_used_local =
                                    bytes_to_capacity_blocks(local_60th);
      }
      else
      {
        // It's not the top of the hour.  We still need to be wary of any
        // drop in the total which would occur if any SAS in the
        // deployment failed or temporarily lost contact with the DCM.
        if (total_bytes_this_hour > last_total_bytes_reported)
        {
          _statistics._capacity_used_total = bytes_to_capacity_blocks(
                        total_bytes_this_hour - last_total_bytes_reported);
          last_total_bytes_reported = total_bytes_this_hour;
        }
        else
        {
          // Report zero for this stats period.  Don't reset the last
          // reported total in this case where the total has dropped - this
          // is probably because one SAS has temporarily lost connectivity
          // with the DCM and it will recover soon - we certainly don't
          // wish to see a spike then!
          _statistics._capacity_used_total = bytes_to_capacity_blocks(0);
        }

        // Repeat logic for the local capacity statistic.
        if (local_bytes_this_hour > last_local_bytes_reported)
        {
          _statistics._capacity_used_local = bytes_to_capacity_blocks(
                        local_bytes_this_hour - last_local_bytes_reported);
          last_local_bytes_reported = local_bytes_this_hour;
        }
        else
        {
          _statistics._capacity_used_local = bytes_to_capacity_blocks(0);
        }
      }

      // capacity_used_third_party is only counted locally.  It's not
      // currently exposed to customers - it only appears on internal
      // stats graphs.
      _statistics._capacity_used_third_party =
        bytes_to_capacity_blocks(
                  _internal_statistics._num_bytes_processed_third_party);

      // Discarded bytes are also only counted locally.
      _statistics._capacity_discarded_licensing =
        bytes_to_capacity_blocks(
                    _internal_statistics._num_bytes_discarded_licensing);

      // Capacity licensed is the total capacity of the deployment.
      _statistics._capacity_licensed         =
        bytes_to_capacity_blocks(def_licensed_bytes_per_hour);

      work_msg_q->reset_stats();
      trail_map_table->_num_trails = 0;
      last_queue_depth = depth;

#ifdef OS_LINUX
      SAS::Stat stat = SAS::Stat(1);
      stat.add_static_param(_statistics._num_received);
      stat.add_static_param(_statistics._num_processed);
      stat.add_static_param(_statistics._num_discarded);
      stat.add_static_param(*reinterpret_cast<uint32*>(&_statistics._max_queue_used_percentage));
      stat.add_static_param(*reinterpret_cast<uint32*>(&_statistics._min_queue_used_percentage));
      SAS::report_stat(stat);
#endif

      if ( _internal_statistics._num_licensing_discards > 0 )
      {
        // Log to Syslog if we are discarding due to licensing.
        TRC_ALT_SYSLOG("SYSTEM Has exceeded its licensed capacity!!!  "
                       "Discarded %d messages",
                       _internal_statistics._num_licensing_discards);
        TRC_NRM_ALWAYS(("Licensed capacity: %lu, number of bytes "
                        "received so far this hour: %lu",
                        def_licensed_bytes_per_hour,
                        (local_discards + local_bytes_this_hour)));
      }

      db_analytics_mgr->calculateStatistics();

      // Now copy all the statistics to one class and pass it to the
      // statistics thread to work on.
      db_statistics_collection * pStats =
        new db_statistics_collection(&_statistics,
                                     trail_mgr->getStatistics(),
                                     trail_table->getStatistics(),
                                     trail_mgr->get_table_statistics(),
                                     trail_mgr->getMarkersPerCallStatistics(),
                                     &_system_statistics,
                                     db_analytics_mgr->getStatistics());
      stats_mgr->queueWork(&pStats);

      // Don't forget to reset the statistics.  Note that the
      // markers-per-type statistics are read and reset in a single call
      // made by the statistics thread, so no need to reset them here.
      TRC_DBG(("Reset the statistics"));
      _internal_statistics.reset();
      _statistics.reset();
      trail_mgr->getStatistics()->reset();
      trail_table->getStatistics()->reset();
      trail_mgr->getMarkersPerCallStatistics()->reset();
      _system_statistics.reset();
      db_analytics_mgr->getStatistics()->reset();

#ifdef DB_TIME
      TRC_NRM_ALWAYS(("DB times %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd %vd",
                       db_time[DB_TIME_FIND_TRAIL],
                       db_time[DB_TIME_COPY],
                       db_time[DB_TIME_CREATE_SUB_TABLE],
                       db_time[DB_TIME_CREATE_INDEX],
                       db_time[DB_TIME_INSERT_TABLE_INFO],
                       db_time[DB_TIME_UPDATE_TABLE_INFO],
                       db_time[DB_TIME_PREPARE_UPDATE],
                       db_time[DB_TIME_BEGIN_UPDATES],
                       db_time[DB_TIME_UPDATE],
                       db_time[DB_TIME_COMMIT_UPDATES],
                       db_time[DB_TIME_WAIT_BUFFER],
                       db_time[DB_TIME_QUERY_OLDEST_EVENT_TABLE],
                       db_time[DB_TIME_QUERY_OLDEST_EVENT],
                       db_time[DB_TIME_INSERT_MV_STATS],
                       db_time[DB_TIME_SET_TIMEZONE],
                       db_time[DB_TIME_INSERT_DASHBOARD_STATS],
                       db_time[DB_TIME_INSERT_SAS_STATS]));

      TRC_NRM(("Reset db_time stats"));

      for (int i = 0; i <= DB_TIME_MAX; i++)
      {
        db_time[i] = 0;
      }
#endif
    }

    msg_count = 0;
  }

  TRC_END_FN();
}

void db_manager::process_msgs()
{
  PGresult *res = NULL;

  TRC_BEGIN_FN("process_msgs");
  char query[256];
  uint64 local_discards = 0;

  // We keep a local count of bytes written this hour and DCM provides us with
  // the total across the deployment.
  uint64 local_bytes_this_hour = 0;
  uint64 total_bytes_this_hour = 0;

  // We build our per-minute statistics of how many bytes we've written by
  // keeping track of the last values we reported and recording the increments
  // in the stats.
  uint64 last_local_bytes_reported = 0;
  uint64 last_total_bytes_reported = 0;

  // At the top of the hour each SAS resets its local count, which in turn
  // lowers the DCM total.  We remember the highest value reached prior to the
  // dip so that we don't record stats for just 59 minutes out of each hour.
  uint64 total_hwm = 0;
  uint64 local_60th = 0;
  uint64 total_60th = 0;
  bool capacity_ok = true;
  int64 hours_since_epoch = db_util::getHoursSinceEpoch();
  uint64 def_licensed_bytes_per_hour = 0;

  // Take a note of the time we start.
  time_t curtime = time(NULL);
  struct tm *loctime = localtime(&curtime);

  // We limit the amount of event data SAS can store hourly according to the
  // number of capacity blocks in the license file.
  //
  // Check whether we have recorded any capacity values so far for this hour on
  // a previous run of SAS.
  double db_total = 0.0;
  double db_local = 0.0;

  db_message_statistics::readCapacityStatsDB(hours_since_epoch * 3600,
                                             &db_total,
                                             &db_local);

  if ((db_total > 0) || (db_local > 0))
  {
    // We have previously worked within this hour.  Any used capacity should be
    // part of our initial reported totals for this hour.
    local_bytes_this_hour = db_local * SIZE_OF_CAPACITY_BLOCK;
    total_bytes_this_hour = db_total * SIZE_OF_CAPACITY_BLOCK;

    last_local_bytes_reported = local_bytes_this_hour;
    last_total_bytes_reported = total_bytes_this_hour;

    total_hwm = total_bytes_this_hour;

    TRC_ERR_SYSLOG("local bytes prior to restart: %lld",local_bytes_this_hour);
    TRC_ERR_SYSLOG("total bytes prior to restart: %lld",total_bytes_this_hour);
  }

  update_licensed_capacity(&def_licensed_bytes_per_hour,
                           &total_bytes_this_hour,
                           local_bytes_this_hour);

  // We must always check if a new high water mark has been reached after
  // updating licensed capacity.
  if (total_hwm < total_bytes_this_hour)
  {
    total_hwm = total_bytes_this_hour;
  }

  uint64 grace = 0;

  // Try to align the stats with the top of the minute.
  int64 last_stat_timestamp =
                            db_util::getTimestamp() - (loctime->tm_sec * 1000);

  // Initialize the sequence number to 1.  This introduces a small risk
  // that some markers and events may be out of sync for calls that were
  // in progress across the restart of the application, but the alternative
  // would be the scan the event table for the largest sequence number,
  // which would be grim.
  _sqn = 1;

  char msg_buf[MAX_MSG_LENGTH + offsetof(db_msg, msg_data)];
  int msg_count = 0;
  bool idle;
  _internal_statistics.reset();
  _last_system_purge_timestamp = _internal_statistics._last_timestamp;
  int64 last_queue_depth = work_msg_q->depth();

#ifdef DB_TIME
  TRC_NRM(("Reset db_time stats"));
  for (int i = 0; i <= DB_TIME_MAX; i++)
  {
    db_time[i] = 0;
  }
#endif

  while (1)
  {
    try
    {
      while (1)
      {
        // START OF MAINLINE MESSAGE PROCESSING LOOP
        //
        // Copy data from the message queue into a local buffer.
        // Wait up to 500ms for a new message.
        idle = true;

        // Get all available messages, sleeping for up to 500ms if no messages
        // are available.
        int new_msg_count = work_msg_q->pop(msg_buf, sizeof(msg_buf), 500);
        int64 new_msg_bytes = 0;
        int64 new_msg_bytes_third_party = 0;

        unsigned int msg_offset = 0;
        TRC_DBG(("Popped %d messages off work queue", new_msg_count));

        // Check if there is sufficient capacity to process these messages.
        // If we have exceeded capacity, then we'll dump all messages except
        // for:
        //
        // * System Connect Messages
        // * System Disconnect Messages
        //
        // We calculate used capacity by totalling the bytes written each hour,
        // but the checks are more complicated because we have to consider the
        // totals written by all the other SASes in the deployment.
        //
        //
        // Role of the DCM
        // ---------------
        //
        // We obtain the overall byte totals by reporting our progress
        // regularly to DCM in the policedog thread.  If we lose contact with
        // the DCM then the remote byte total will be out of date and we may
        // use our local value for the checks instead.  Naturally, the local
        // value can only exceed the remote total if we remain out of contact.
        //
        // [This allows a dishonest customer to achieve (num_instances *
        // capacity) for the duration of the grace period by deliberately
        // disconnecting the DCM from the network.]
        //
        //
        // Calculation
        // -----------
        //
        // We need to take into account how many bytes are licensed for this
        // hour and how many have been used both locally and remotely.
        //
        // Can we carry on writing event data?  YES, if both:
        //
        // - local_bytes_this_hour has not reached def_licensed_bytes_per_hour
        //
        // - total_bytes_this_hour has not reached def_licensed_bytes_per_hour
        //
        // [We used to have a headroom factor of 1.1 so that we would only
        // actually discard events when the customer was at 110% capacity, but
        // DCM refuses to count past the limit so we've stopped doing that!]
        if ((local_bytes_this_hour >= def_licensed_bytes_per_hour) ||
            (total_bytes_this_hour >= def_licensed_bytes_per_hour)    )
        {
          TRC_NRM(("CAPACITY BREACHED!!!"));
          TRC_DBG(("local_bytes_this_hour %lld", local_bytes_this_hour));
          TRC_DBG(("total_bytes_this_hour %lld", total_bytes_this_hour));
          capacity_ok = false;
        }
        else
        {
          // This block is needed in the case where a new capacity license is
          // installed part way through the hour, which is larger than the
          // previous one.
          // It also copes with resetting capacity_ok at the start of the hour.
          TRC_DBG(("Capacity Ok"));
          capacity_ok = true;
        }

        // Refresh the licensed capacity.  The underlying implementation makes
        // sure we only read the license file from disk at most once a minute,
        // but we need to keep up-to-date on the written byte totals (and that
        // is true even on Solaris!).
        update_licensed_capacity(&def_licensed_bytes_per_hour,
                                 &total_bytes_this_hour,
                                 local_bytes_this_hour);

        // Check if the current total represents a high water mark for this
        // hour.  We want to avoid claiming no capacity has been used for the
        // last minute of the hour.
        if (total_hwm < total_bytes_this_hour)
        {
          total_hwm = total_bytes_this_hour;
        }

#ifdef LICENSING
        grace = licensed_grace.load();
#endif

        for (int ii = 0; ii < new_msg_count; ii++)
        {
          // Process the messages returned in the buffer.
          db_msg* msg = (db_msg*) &msg_buf[msg_offset];
          msg_offset += (msg->msg_length + offsetof(db_msg, msg_data));

          if (msg->msg_type == db_msg::MSG_RECEIVED ||
              msg->msg_type == db_msg::THRD_PRTY_MSG_RECEIVED)
          {
            // Received a message from an NE system, so process it.
            idle = false;

            if (msg->radioactive != 0)
            {
              // We have got a radioactive message off the message queue.
              RADIOACTIVE_LOG("got radioactive message %u from the message queue",
                              msg->radioactive);
            }

            if (IS_VALID_MSG_VERSION(msg->msg_data))
            {
              bool write_to_sas = true;

              switch (GET_MSG_HDR_TYPE(msg->msg_data))
              {
                case MSG_EVENT:
                case MSG_MARKER:
                case MSG_ASSOC:
                  // Analytics events should always be forwarded, regardless of
                  // whether there has been a capacity breach.  Similarly, they
                  // should not count towards licensing limits unless they are
                  // actually being written to SAS.
                  if (GET_MSG_HDR_TYPE(msg->msg_data) == MSG_EVENT)
                  {
                    write_to_sas = process_analytics_event(msg);
                  }

                  if (write_to_sas)
                  {
                    // As this message is being written to SAS, keep track of
                    // its size to track its effect on licensed capacity.
                    new_msg_bytes += msg->msg_length;
                    TRC_DBG(("new_msg_bytes: %ld", new_msg_bytes));

                    if (msg->msg_type == db_msg::THRD_PRTY_MSG_RECEIVED)
                    {
                      new_msg_bytes_third_party += msg->msg_length;
                      TRC_DBG(("new_msg_bytes_third_party: %ld",
                               new_msg_bytes_third_party));
                    }

                    if ((grace == 1) || !capacity_ok)
                    {
                      // We're not correctly licensed to store this message.
                      TRC_DBG(("No grace or capacity breached!"));

                      if (!capacity_ok)
                      {
                        // Keep an accurate count of breached capacity stats.
                        TRC_DBG(("Breached capacity! Discarding message"));
                        _internal_statistics._num_licensing_discards++;
                        _internal_statistics._num_bytes_discarded_licensing +=
                                                               msg->msg_length;
                      }
                    }
                    else
                    {
                      switch (GET_MSG_HDR_TYPE(msg->msg_data))
                      {
                        case MSG_EVENT:
                          process_event(msg);
                          break;

                        case MSG_MARKER:
                          process_marker(msg);
                          break;

                        case MSG_ASSOC:
                          process_assoc(msg);
                          break;
                      }
                    }
                  }
                  break;

                case MSG_STAT:
                  process_stat(msg);
                  break;

                case MSG_HEARTBEAT:
                  TRC_ERR_SYSLOG("Received heartbeat message from system %d "
                                 "that should have been ignored",
                                 msg->system_id);
                  break;

                default:
                  TRC_ERR_SYSLOG("Received message with unknown type %d "
                                 "from system with ID %d",
                                 GET_MSG_HDR_TYPE(msg->msg_data),
                                 msg->system_id);
                  TRC_DATA_ERR("Message: ", msg, msg->msg_length);
                  break;
              }
            }
          }
          else if (msg->msg_type == db_msg::SYSTEM_CONNECTED)
          {
            // System newly connected, need to inform trail table.
            // We do this regardless of capacity limits
            TRC_NRM(("Received system connected message"));
            trail_table->system_connected(msg->system_id);
            _system_statistics.add_system_connection(msg->system_id);
          }
          else if (msg->msg_type == db_msg::SYSTEM_DISCONNECTED)
          {
            // System has disconnected, need to inform trail table.
            TRC_NRM(("Received system disconnected message"));
            trail_table->system_disconnected(msg->system_id);
            _system_statistics.add_system_disconnection(msg->system_id);

            // Record the time that the system disconnected from SAS.
            sprintf(query,
                    "UPDATE systems SET time_disconnected=current_timestamp "
                    "WHERE id=%d;",
                    msg->system_id);
            res = PQexec(db_conn(), query);

            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
              TRC_ERR_SYSLOG("PQexec(update system) failed, "
                             "status = %d\n  error: %s\n  %s",
                             PQresultStatus(res),
                             PQresultErrorMessage(res),
                             query);
            }
          }
          else
          {
            TRC_ERR_SYSLOG("Unrecognised message type %hd", msg->msg_type);
          }
        }

        // Increment count of messages.
        _internal_statistics._num_messages += new_msg_count;
        _internal_statistics._num_bytes_processed += new_msg_bytes;
        _internal_statistics._num_bytes_processed_third_party +=
                                                     new_msg_bytes_third_party;

        if (capacity_ok)
        {
          // Only count the bytes written out if we were given the permission,
          // otherwise the deployment-wide counts will go awry.
          local_bytes_this_hour += new_msg_bytes;
        }
        else
        {
          // We are discarding these bytes.
          local_discards += new_msg_bytes;
        }

        msg_count += new_msg_count;

        int64 timestamp = db_util::getTimestamp();

        maybe_commit_tgs(timestamp);
        maybe_flush_tables(idle, timestamp);
        maybe_purge_systems(timestamp);
        maybe_do_statistics(local_60th,
                            local_bytes_this_hour,
                            last_local_bytes_reported,
                            total_60th,
                            total_hwm,
                            last_total_bytes_reported,
                            total_bytes_this_hour,
                            local_discards,
                            def_licensed_bytes_per_hour,
                            last_queue_depth,
                            last_stat_timestamp,
                            hours_since_epoch,
                            timestamp,
                            msg_count);
      }
    }
    catch (int e)
    {
      TRC_ERR_SYSLOG("Database error, attempting to reconnect");
      PQfinish(_db_conn);
      db_connect();
    }
  }

  TRC_END_FN();
}

bool db_manager::process_analytics_event(db_msg* msg)
{
  bool write_event_to_sas = true;

  TRC_BEGIN_FN("process_analytics_event");
  char *event_msg = msg->msg_data;
  system_info* sys_info = NULL;

  // Zero is an invalid system id, so do not allow its use.
  if (msg->system_id == 0)
  {
    TRC_ERR_SYSLOG("Invalid system_id 0 in event");
    goto EXIT_LABEL;
  }

  sys_info = get_system_info(msg->system_id);

  if (sys_info == NULL)
  {
    TRC_ERR_SYSLOG("system_id in event unknown: %d", msg->system_id);
    goto EXIT_LABEL;
  }

  // Check if this event should be logged to analytics by checking its resource
  // identifier and event ID.
  if (db_analytics_mgr->is_analytics_enabled() &&
      db_analytics_mgr->is_analytics_event(sys_info, event_msg))
  {
    TRC_DBG(("Forwarding message to analytics queue (%s/%s/%X)",
             sys_info->_resource_identifier.c_str(),
             sys_info->_system_type.c_str(),
             db_util::get_int32(GET_EVENT_MSG_EVENT_ID(event_msg))));

    analytics_msg_q->push(msg);
    write_event_to_sas = db_analytics_mgr->is_sas_event(sys_info, event_msg);
  }

EXIT_LABEL:
  TRC_END_FN();
  return write_event_to_sas;
}

void db_manager::process_event(db_msg* msg)
{
  TRC_BEGIN_FN("process_event");
  char *event_msg = msg->msg_data;
  unsigned char* system_key = NULL;
  system_info* sys_info = NULL;

  // Zero is an invalid system id, so do not allow its use.
  if (msg->system_id == 0)
  {
    TRC_ERR_SYSLOG("Invalid system_id 0 in event");
    goto EXIT_LABEL;
  }

#ifdef LICENSING
  sys_info = get_system_info(msg->system_id);

  if (sys_info == NULL)
  {
    TRC_ERR_SYSLOG("system_id in event unknown: %d", msg->system_id);
    goto EXIT_LABEL;
  }

  system_key = sys_info->_system_key;
#endif

  _internal_statistics._num_events++;

  TRC_DBG(("Process event %X (instance %d) from system %u on trail %vd",
           db_util::get_int32(GET_EVENT_MSG_EVENT_ID(event_msg)),
           db_util::get_int32(GET_EVENT_MSG_INST_ID(event_msg)),
           msg->system_id,
           db_util::get_int64(GET_EVENT_MSG_TRAIL_ID(event_msg))));
  event_table->add(_sqn++,
                   msg->system_id,
                   db_util::get_int64(GET_EVENT_MSG_TRAIL_ID(event_msg)),
                   db_util::get_int32(GET_EVENT_MSG_EVENT_ID(event_msg)),
                   db_util::get_int32(GET_EVENT_MSG_INST_ID(event_msg)),
                   db_util::get_int64(GET_MSG_HDR_TIMESTAMP(event_msg)),
                   msg->msg_length -
                            (GET_EVENT_MSG_DATA_OFFSET(event_msg) - event_msg),
                   GET_EVENT_MSG_DATA_OFFSET(event_msg),
                   msg->radioactive,
                   system_key);

EXIT_LABEL:
  TRC_END_FN();
  return;
}

bool db_manager::write_marker_to_db(int marker_id)
{
  bool write;
  TRC_BEGIN_FN("write_marker_to_db");

  bool supressed = false;
  int j;
  for (j = 0; j < _dont_write_markers_len; j++)
  {
    if (marker_id == _dont_write_markers[j])
    {
      TRC_DBG(("Ignore marker with ID %d", marker_id));
      supressed = true;
    }
  }

  write = (((marker_id) != MARKER_ID_CALL_INDEX) &&
           ((marker_id) != MARKER_ID_ICC_CALL_INDEX) &&
           ((marker_id) != MARKER_ID_ICC_BRANCH_INDEX) &&
           ((marker_id) != MARKER_ID_SB_SESSION_ID_CORRELATOR) &&
           ((marker_id) != MARKER_ID_FLUSH) &&
           ((marker_id) != MARKER_ID_SIP_AUTH_CORRELATOR) &&
           (!IS_FEDERATION_CORRELATING_MARKER(marker_id) ||
           write_federation_correlators) &&
           !supressed);

  TRC_END_FN();
  return write;
}

void db_manager::process_marker(db_msg* msg)
{
  TRC_BEGIN_FN("process_marker");
  char* marker_msg = msg->msg_data;
  db_trail* trail;
  int32 marker_id;
  int32 inst_id;
  int64 timestamp;
  int8 assoc_ops = GET_MARKER_MSG_ASSOCIATION(marker_msg);
  unsigned char* system_key = NULL;
  system_info* sys_info;

  // Ensure the scope has a defined value if the marker is non-associative.
  int8 scope = (((assoc_ops & ASSOC_OP_ASSOCIATE) == 0) ?
                  ASSOC_SCOPE_NONE :
                  GET_MARKER_MSG_SCOPE(marker_msg));

  // Zero is an invalid system id, so do not allow its use.
  if (msg->system_id == 0)
  {
    TRC_ERR_SYSLOG("Invalid system_id 0 in marker");
    goto EXIT_LABEL;
  }

#ifdef LICENSING
  sys_info = get_system_info(msg->system_id);
  if (sys_info == NULL)
  {
    TRC_ERR_SYSLOG("system_id in marker unknown: %d", msg->system_id);
    goto EXIT_LABEL;
  }

  system_key = sys_info->_system_key;
#endif

  _internal_statistics._num_markers++;

  trail = trail_mgr->load_trail(msg->system_id,
               db_util::get_int64(GET_MARKER_MSG_TRAIL_ID(marker_msg)));

  marker_id = db_util::get_int32(GET_MARKER_MSG_MARKER_ID(marker_msg));
  inst_id = db_util::get_int32(GET_MARKER_MSG_INST_ID(marker_msg));
  timestamp = db_util::get_int64(GET_MSG_HDR_TIMESTAMP(marker_msg));

  TRC_DBG(("Process marker %X (instance %d) from system %u on trail id %vd",
           marker_id, inst_id, msg->system_id, trail->_trail_id));

  if (write_marker_to_db(marker_id))
  {
    // This is a marker we want to store in the database for search and
    // decode.
    if (msg->radioactive != 0)
    {
      // Marker is radioactive - log adding the marker to the trail
      RADIOACTIVE_LOG("radioactive message %u is a marker with id %x "
                      "being added to trail %lld",
                      msg->radioactive, marker_id, trail->_trail_id);
    }

    TRC_DBG(("Add marker into the bloom filter"));
    db_marker_cache->insert_into(
        1, GET_MARKER_MSG_DATA_OFFSET(marker_msg),
        msg->msg_length -
            (GET_MARKER_MSG_DATA_OFFSET(marker_msg) - marker_msg));

    trail->add_marker(_sqn++,
                      marker_id,
                      inst_id,
                      timestamp,
                      msg->msg_length -
                         (GET_MARKER_MSG_DATA_OFFSET(marker_msg) - marker_msg),
                      GET_MARKER_MSG_DATA_OFFSET(marker_msg),
                      scope,
                      msg->radioactive,
                      msg->encoding);
  }

  if ((assoc_ops & ASSOC_OP_ASSOCIATE) != 0)
  {
    TRC_DBG(("Marker is association marker"));

    if (!this->is_event_id_in_ignore_list(EVENT_ID_MARKER_ASSOC))
    {
      // Add an event when there is an association marker in order to be able
      // debug trail group associations.
      TRC_DBG(("Add association marker debug event"));
      event_table->add(_sqn++,
                       msg->system_id,
                       db_util::get_int64(GET_MARKER_MSG_TRAIL_ID(marker_msg)),
                       EVENT_ID_MARKER_ASSOC,
                       inst_id,
                       timestamp,
                       msg->msg_length -
                         (GET_MARKER_MSG_DATA_OFFSET(marker_msg) - marker_msg),
                       GET_MARKER_MSG_DATA_OFFSET(marker_msg),
                       0,
                       system_key);
    }

    // Work out whether to reactivate 'ended' trail groups.
    bool reactivate_tg = true;

    if ((assoc_ops & ASSOC_OP_NO_REACTIVATE) != 0)
    {
      // Marker message explicitly requests that the trail group should not be
      // reactivated.
      reactivate_tg = false;
    }
    else if (marker_id == MARKER_ID_MG_CORRELATOR)
    {
      // Don't reactivate trails between the the CFS and UMG - for analog lines
      // there is a set of associations after the call has ended as the CFS
      // communicates tear-down messages to the UMG that otherwise would wrongly
      // reactivate the ended trail group.
      reactivate_tg = false;
    }

    // See if this marker matches another that dbapp has already received.
    trail_mgr->marker_association(trail,
                                  marker_id,
                                  msg->msg_length -
                         (GET_MARKER_MSG_DATA_OFFSET(marker_msg) - marker_msg),
                                  GET_MARKER_MSG_DATA_OFFSET(marker_msg),
                                  scope,
                                  msg->system_id,
                                  inst_id,
                                  timestamp,
                                  _sqn++,
                                  system_key,
                                  reactivate_tg);
  }

  if ((marker_id == MARKER_ID_END_TIME) ||
      (marker_id == MARKER_ID_FLUSH))
  {
    // To speed up dumping to the database move the trail group to the
    // 'ended' list.  TGs on the ended list are committed more rapidly than
    // those on the active list - see commit_tgs.  (Commits write first to the
    // table classes, then on through the copy buffers to the database.)
    TRC_DBG(("Mark trail group %vd as ended", trail->_tg_id));
    trail_mgr->tg_ended(trail->_tg);
  }

  if (marker_id == MARKER_ID_END_TIME)
  {
    // We use the end time marker to indicate the number of calls processed.
    // This is only an approximate measure, and some common scenarios add more
    // than one end time marker to a trail group (e.g. CFS and EAS both
    // pointing at the same SAS).
    TRC_DBG(("Record a newly ended call"));
    _internal_statistics._num_calls++;
  }

EXIT_LABEL:
  TRC_END_FN();
  return;
}

void db_manager::process_assoc(db_msg* msg)
{
  TRC_BEGIN_FN("process_assoc");
  char* assoc_msg = msg->msg_data;
  db_trail* trail1;
  db_trail* trail2;

  if(msg->system_id == 0)
  {
    TRC_ERR_SYSLOG("Invalid system_id 0 in association");
    goto EXIT_LABEL;
  }

  trail1 = trail_mgr->load_trail(msg->system_id,
                                 db_util::get_int64(GET_ASSOC_MSG_TRAIL_ID1(assoc_msg)));
  trail2 = trail_mgr->load_trail(msg->system_id,
                                 db_util::get_int64(GET_ASSOC_MSG_TRAIL_ID2(assoc_msg)));

  TRC_DBG(("Associate trails %vd and %vd", trail1->_trail_id, trail2->_trail_id));
  trail_mgr->associate_trails(trail1, trail2, GET_ASSOC_MSG_SCOPE(assoc_msg), true);

  _internal_statistics._num_association_messages++;

EXIT_LABEL:
  TRC_END_FN();
  return;
}

void db_manager::process_stat(db_msg* msg)
{
  TRC_BEGIN_FN("process_stat");
  char *stat_msg = msg->msg_data;

  // Zero is an invalid system id, so do not allow its use.
  if(msg->system_id == 0)
  {
    TRC_ERR_SYSLOG("Invalid system_id 0 in stat");
    goto EXIT_LABEL;
  }

  TRC_DBG(("Process stat %X from system %u",
           db_util::get_int32(GET_STAT_MSG_STAT_ID(stat_msg)),
           msg->system_id));
  stat_table->add(msg->system_id,
                  db_util::get_int32(GET_STAT_MSG_STAT_ID(stat_msg)),
                  db_util::get_int64(GET_MSG_HDR_TIMESTAMP(stat_msg)),
                  msg->msg_length -
                           (GET_STAT_MSG_DATA_OFFSET(stat_msg) - stat_msg),
                  GET_STAT_MSG_DATA_OFFSET(stat_msg),
                  msg->radioactive);

EXIT_LABEL:
  TRC_END_FN();
  return;
}

void db_manager::commit_tgs(int64 timestamp)
{
  TRC_BEGIN_FN("commit_tgs");

  // Commit any active trail groups that have not been touched in the last five
  // minutes, and any 'ended' trail groups (where we have seen an end time
  // marker) that have not been touched in the last 5 seconds.
  int tg_count = trail_mgr->commit_lru(timestamp - 5*60*1000,
                                       timestamp -    5*1000);
  TRC_DBG(("Flushed %d trail groups", tg_count));

  // Purge any marker associations that have been around for more than 60
  // seconds.
  trail_mgr->purge_marker_associations(timestamp - 60*1000);

  int64 flush_timestamp = db_util::getTimestamp();

  if (flush_timestamp > (timestamp + 500))
  {
    TRC_NRM_ALWAYS(("Slow trail group commit: took %vdms, flushed %d TGs",
                    flush_timestamp - timestamp,
                    tg_count));
  }

  TRC_END_FN();
}

void db_manager::flush_tables()
{
  TRC_BEGIN_FN("flush_tables");
  TRC_DBG(("Flush all tables"));

  trail_table->flush();
  event_table->flush();
  stat_table->flush();
  tg_table->flush();

  for (map<int,db_marker_table*>::iterator i = _marker_map.begin();
       i != _marker_map.end();
       i++)
  {
    (i->second)->flush();
  }

  TRC_END_FN();
}

// Flushes the tables that have not been flushed since the supplied timestamp.
void db_manager::flush_tables(int64 timestamp)
{
  TRC_BEGIN_FN("flush_tables");

  trail_table->flush(timestamp);
  event_table->flush(timestamp);
  stat_table->flush(timestamp);
  tg_table->flush(timestamp);

  for (map<int,db_marker_table*>::iterator i = _marker_map.begin();
       i != _marker_map.end();
       i++)
  {
    (i->second)->flush(timestamp);
  }

  TRC_END_FN();
}

PGconn* db_manager::db_conn()
{
  TRC_BEGIN_FN("db_conn");

  if (_db_conn)
  {
    if (PQstatus(_db_conn) != CONNECTION_OK)
    {
      TRC_ERR_SYSLOG("Reconnecting to database");
      db_connect();
    }
  }

  TRC_END_FN();
  return _db_conn;
}

void db_manager::get_and_reset_table_stats(
                                        db_marker_stats_container &msg_stats,
                                        table_stats               &trail_stats,
                                        table_stats               &event_stats,
                                        table_stats               &stat_stats,
                                        table_stats               &tg_stats)
{
  TRC_BEGIN_FN("get_and_reset_table_stats");

  TRC_DBG(("Resetting table stats - markers"));

  for (map<int, db_marker_table*>::iterator it = _marker_map.begin();
       it != _marker_map.end();
       it++)
  {
    // Get and reset the marker count for this particular marker table.
    db_marker_table *tab = it->second;
    int marker_id = it->first;

    TRC_DBG(("Resetting table stats for marker number %X", marker_id));

    msg_stats[marker_id] = tab->get_and_reset_table_stats();
  }

  TRC_DBG(("Resetting table stats - auxilliary"));

  trail_stats = trail_table->get_and_reset_table_stats();
  event_stats = event_table->get_and_reset_table_stats();
  stat_stats  =  stat_table->get_and_reset_table_stats();
  tg_stats    =    tg_table->get_and_reset_table_stats();

  TRC_DBG(("Finished resetting table stats"));

  TRC_END_FN();
}

void db_manager::linux_scale_ram(int &ram_scale_percent)
{
  TRC_BEGIN_FN("linux_scale_ram");

#ifdef OS_LINUX
  struct sysinfo info;
  sysinfo(&info);

  // The minimum amount of RAM for a production SAS is 8 GB.  The sysinfo call
  // will return the MemTotal value from /proc/meminfo, which will be slightly
  // less than 8 GB.  For this reason, we put the cut-off at 7 GB.
  if (info.totalram < 7UL*1024UL*1024UL*1024UL)
  {
    // This system has less than the minimum amount of RAM, so won't be dealing
    // with any significant load.  Scale down the memory usage to 5% (minimum).
    TRC_DBG(("System has less than 8 GB RAM"));

    TRC_NRM_SYSLOG("This is a lab-spec SAS (<8GB of RAM detected)");
    ram_scale_percent = 5;
  }
  else
  {
    // This system has at least 8 GB RAM.  Scale the memory usage relative to
    // how much extra there is.
    TRC_DBG(("System has at least 8 GB RAM"));

    long totalram = max(info.totalram, 8UL*1024UL*1024UL*1024UL);
    ram_scale_percent = (totalram * 100L) / (8*1024UL*1024UL*1024UL);
  }
#endif

  TRC_END_FN();
}

int db_manager::calculate_tg_commit_period()
{
  int res;
  TRC_BEGIN_FN("calculate_tg_commit_period");

  res = db_util::get_and_validate_config_int(
                                  db_util::CONFIG_TRAIL_GROUP_COMMIT_PERIOD_MS,
                                  5,                            // Minimum.
                                  10000,                        // Maximum.
                                  DEFAULT_TG_COMMIT_PERIOD_MS); // Default.

  TRC_END_FN();
  return res;
}

int db_manager::calculate_table_flush_period()
{
  int res;
  TRC_BEGIN_FN("calculate_table_flush_period");

  res = db_util::get_and_validate_config_int(
                                         db_util::CONFIG_TABLE_FLUSH_PERIOD_MS,
                                         500,
                                         300000,
                                         DEFAULT_TABLE_FLUSH_PERIOD_MS);

  TRC_END_FN();
  return res;
}

int db_manager::calculate_stats_check_period()
{
  int res;
  TRC_BEGIN_FN("calculate_stats_check_period");

  res = db_util::get_and_validate_config_int(
                                         db_util::CONFIG_STATS_CHECK_PERIOD_MS,
                                         5,
                                         10000,
                                         DEFAULT_STATS_CHECK_PERIOD_MS);

  TRC_END_FN();
  return res;
}

void db_manager::setup_perf_parameters()
{
  TRC_BEGIN_FN("setup_perf_parameters");

  // Calculate System RAM on Linux.  On Solaris, just hardcode the RAM scale to
  // 100%.
  int ram_scale_percent = 100;
  linux_scale_ram(ram_scale_percent);

  // Read in database performance factors, and validate.
  // We use 2 inputs here:
  //
  // SCALE_PERCENT is a rougth measure of the proportion of the boxes
  // resources SAS should use.
  //
  // ram_scale - this is the amount of RAM this box has relative to a 8GB #
  // UX4410.
  // From this we calculate 2 variables:
  //
  // * scale - this is used for parmeters that we want to scale fully linearly
  //
  // * scale_with_mul_3_percent - used to scale parameters that we do not want
  // to reduce so severely when scale factor is less than 100%
  // This is used for sub-table sizes
  // to provide a trade-off between subtables spanning a relatively small time
  // range, and excess database overhead
  scale_percent = db_util::get_and_validate_config_int(
                      db_util::CONFIG_DB_SCALE_PERCENT,
                      5,                  // Min
                      10000,              // Max
                      ram_scale_percent); // Default

  int scale_with_mul_3_percent;

  if (scale_percent < 100)
  {
    scale_with_mul_3_percent = min(scale_percent * 3, 100);
  }
  else
  {
    scale_with_mul_3_percent = scale_percent;
  }
  TRC_NRM_SYSLOG("Setting Scale Factor to %d percent.  This may be overridden "
                  "by other config settings, which will be logged below",
                  scale_percent);

  // We now setup the values of individual performance parameters.
  // These fall into 4 groups:
  // * The Trail/Trail group caches
  // * The Message queue
  // * The Write buffers
  // * The DB Table sizes.
  // For each of these groups, a scale factor can be specified to override the
  // global scale factor.
  trail_cache_scale_percent = db_util::get_and_validate_config_int(
                    db_util::CONFIG_TRAIL_CACHE_SIZE_PERCENT,
                    5,              // Min
                    10000,          // Max
                    scale_percent); // Default
  write_buffer_scale_percent = db_util::get_and_validate_config_int(
                    db_util::CONFIG_EVENT_WRITE_BUFFER_SIZE_PERCENT,
                    5,
                    10000,
                    scale_with_mul_3_percent);
  db_table_scale_percent = db_util::get_and_validate_config_int(
                    db_util::CONFIG_EVENT_DB_TABLE_SIZE_PERCENT,
                    10,
                    1000000,
                    scale_with_mul_3_percent);

  int message_queue_scale_percent = db_util::get_and_validate_config_int(
                    db_util::CONFIG_MESSAGE_QUEUE_SIZE_PERCENT,
                    5,
                    10000,
                    scale_percent);
  message_queue_size = db_util::scale_param(DEFAULT_EVENT_QUEUE_SIZE,
                                            message_queue_scale_percent);

  trail_group_commit_period_ms = calculate_tg_commit_period();
  table_flush_period_ms        = calculate_table_flush_period();
  stats_check_period_ms        = calculate_stats_check_period();

  TRC_END_FN();
}

void db_manager::system_connected(int32 system_id, system_info sys_info)
{
  TRC_BEGIN_FN("system_connected");
  pthread_mutex_lock(&_system_map_mutex);

  sys_info._disconnect_time = 0;
  _system_map.insert(pair<int32,db_manager::system_info>(system_id, sys_info));

  pthread_mutex_unlock(&_system_map_mutex);
  TRC_END_FN();
}

// Flag that a remote system has disconnected, and tidy up resources for
// systems that disconnected at least 10 minutes ago.
// Resources aren't freed immediately because the information about this
// system might still be needed by messages currently in one of the queues.
void db_manager::system_disconnected(int32 system_id)
{
  TRC_BEGIN_FN("system_disconnected");

  int64 time_now = db_util::getTimestamp();

  // Mark the time that the given system disconnected.
  system_info* sys_info = get_system_info(system_id);
  if (sys_info != NULL)
  {
    TRC_DBG(("Record system disconnection at %d", time_now));
    sys_info->_disconnect_time = time_now;
  }

  // Loop over the table deleting entries for systems that disconnected at
  // least 10 minutes ago. This is because we can't delete systems immediately
  // because that information might still be needed by messages still on the
  // message or analytics queues, and we have no way to tell when they will no
  // longer be needed. Deleting them after a reasonable timeout is a pragmatic
  // compromise.
  pthread_mutex_lock(&_system_map_mutex);

  for (map<int32, system_info>::iterator itr = _system_map.begin();
       itr != _system_map.end();)
  {
    if ((itr->second._disconnect_time != 0) &&
        (time_now > (itr->second._disconnect_time + (10 * 60 * 1000))))
    {
      TRC_DBG_ALWAYS(("Remove system entry for old system (%d)", itr->first));
      _system_map.erase(itr++);
    }
    else
    {
      ++itr;
    }
  }

  pthread_mutex_unlock(&_system_map_mutex);
  TRC_END_FN();
}

db_manager::system_info* db_manager::get_system_info(int32 system_id)
{
  system_info* sys_info;
  TRC_BEGIN_FN("get_system_info");
  pthread_mutex_lock(&_system_map_mutex);

  map<int32, system_info>::iterator itr = _system_map.find(system_id);
  if (itr != _system_map.end())
  {
    TRC_DBG(("Found entry for remote system %d", system_id));
    sys_info = &(itr->second);
  }
  else
  {
    TRC_DBG_ALWAYS(("system_id unknown: %d", system_id));
    sys_info = NULL;
  }

  pthread_mutex_unlock(&_system_map_mutex);
  TRC_END_FN();
  return sys_info;
}

void db_message_statistics::log_internal(int duration_in_seconds)
{
  TRC_BEGIN_FN("log_internal");

  if (_num_discarded > 0)
  {
    // _num_calls is casted to a uint64 to avoid overflow problems.
    TRC_NRM_SYSLOG("Message stats (%d minute%s) - "
                   "received %u (%u/sec), "
                   "received %llu KB (%llu KB/sec), "
                   "processed %u (%u/sec), discarded %u, "
                   "discarded due to licensing %u, "
                   "max queue %u (%f%%), min queue %u (%f%%) "
                   "(~%llu BHCA), update count %u",
                   duration_in_seconds / 60,
                   (duration_in_seconds == 60) ? "" : "s",
                   _num_received,
                   _num_received/duration_in_seconds,
                   _bytes_received/1024,
                   _bytes_received/1024/duration_in_seconds,
                   _num_processed,
                   _num_processed/duration_in_seconds,
                   _num_discarded,
                   _num_discarded_licensing,
                   _max_queue_length,
                   _max_queue_used_percentage,
                   _min_queue_length,
                   _min_queue_used_percentage,
                   ((uint64)_num_calls*60*60)/duration_in_seconds,
                   _num_updates);
  }
  else
  {
    // _num_calls is casted to a uint64 to avoid overflow problems.
    TRC_NRM_ALWAYS(("Message stats (%d minute%s) - received %u (%u/sec), "
                    "received %vu KB (%vu KB/sec), "
                    "processed %u (%u/sec), discarded %u, "
                    "discarded due to licensing %u, "
                    "max queue %u (%d.%.6d%%), min queue %u (%d.%.6d%%) "
                    "(~%vu BHCA), update count %u",
                    duration_in_seconds / 60,
                    (duration_in_seconds == 60) ? "" : "s",
                    _num_received,
                    _num_received/duration_in_seconds,
                    _bytes_received/1024,
                    _bytes_received/1024/duration_in_seconds,
                    _num_processed,
                    _num_processed/duration_in_seconds,
                    _num_discarded,
                    _num_discarded_licensing,
                    _max_queue_length,
                    FLOAT_TO_6DP(_max_queue_used_percentage),
                    _min_queue_length,
                     FLOAT_TO_6DP(_min_queue_used_percentage),
                    ((uint64)_num_calls*60*60)/duration_in_seconds,
                    _num_updates));
  }

  TRC_END_FN();
}

void db_system_statistics::log_internal(int duration_in_seconds)
{
  TRC_BEGIN_FN("log_internal");
  TRC_DBG_ALWAYS((
    "%d system connects/disconnects reported to system statistics class",
    _system_stats.size()
  ));

  TRC_END_FN();
}

int db_manager::trail_cache_scale_percent;
int db_manager::write_buffer_scale_percent;
int db_manager::db_table_scale_percent;
int db_manager::message_queue_size;
int db_manager::trail_group_commit_period_ms;
int db_manager::table_flush_period_ms;
int db_manager::stats_check_period_ms;
