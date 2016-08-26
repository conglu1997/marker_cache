#include <dbinc.h>
#include <sstream>
#include <vector>

int main() {
  // This code will work to build up the current filter in case of a crash,
  // building up many is going to need some hooks into marker_cache

  std::vector<std::string> table_names;

  for (map<int, db_marker_table *>::iterator i = _marker_map.begin();
       i != _marker_map.end(); i++) {
    table_names.push_back((i->second)->getName());
  }

  // TODO: Get actual start and endpoints from marker_cache
  time_t start = time(NULL) - 1000;
  time_t end = time(NULL);

  char sbuf[20];
  strftime(sbuf, sizeof sbuf, "%F %T", gmtime(&start));
  char ebuf[20];
  strftime(ebuf, sizeof ebuf, "%F %T", gmtime(&end));

  for (std::vector<std::string>::iterator i = table_names.begin();
       i != table_names.end(); ++i) {
    PGresult *res;
    char query[2048];

    // Find the IDs for the subtables which overlap our search period.
    sprintf(query, "SELECT id FROM %s_tables WHERE min_init_time <= %s AND %s "
                   "<= max_init_time",
            i->c_str(), ebuf, sbuf);
    res = PQexec(db_conn(), query);

    int tuple_count = 0;
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      TRC_ALT_SYSLOG("Error trying to rebuild filter");
    } else {
      tuple_count = PQntuples(id_search_results);
      TRC_NRM(("Found %d %s subtables in the search period", tuple_count,
               i->c_str()));
    }

    for (int j = 0; j < PQntuples(res); ++j) {
      // Extract the table IDs and pad them to 8 digits
      int64_t table_id = db_util::atoi64(PQgetvalue(res, j, 0));
      std::stringstream ss;
      ss << table_id;
      std::string padded_id;
      ss >> padded_id;
      while (padded_id.length() < 8) {
        padded_id = '0' + padded_id;
      }
      sprintf(query, "SELECT * FROM %s_%s WHERE init_time BETWEEN %s AND %s",
              i->c_str(), padded_id.c_str(), sbuf, ebuf);

      PGresult *mres;
      mres = PQexec(db_conn(), query);

      // Process the markers that need to be inserted into the Bloom filter
      int mtuple_count = 0;
      if (PQresultStatus(mres) != PGRES_TUPLES_OK) {
        TRC_ALT_SYSLOG("Error trying to retrieve markers");
      } else {
        mtuple_count = PQntuples(id_search_results);
        TRC_NRM(
            ("Found %d markers in the %s subtables", mtuple_count, i->c_str()));
      }

      for (int k = 0; k < PQntuples(mres); ++k) {
        // TODO: Need the correct offset for each value
        // Each marker type has a different offset
        int offset = 0;
        db_marker_cache->insert(PQgetvalue(res, k, offset));
      }

      PQclear(mres);
    }

    PQclear(res);
  }
}
