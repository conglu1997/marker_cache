#include <syslog.h>
#include <errno.h>
#include <signal.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include <sys/wait.h>
#include <limits.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <dirent.h>
#include <netdb.h>

#ifdef OS_LINUX
#include <sys/sysinfo.h>
#endif

// Common STL includes.
#include <cassert>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <ctime>

// Include the SAS library on Linux so that there is an option of sending
// stats to a remote SAS.  There isn't currently a build of the SAS client
// library on Solaris.
#ifdef OS_LINUX
#include <sas.h>
#endif

// And a custom implementation of queue, because Solaris' implementation
// is very buggy (memory corruption - see SFR 437699)
#ifdef OS_LINUX
#include <queue>
#else
#include <myque>
#include <atomic.h>
#endif

// Add openssl headers to be able to encypt data before storing events for when
// this is using the licensing code.
#ifdef LICENSING
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#endif

// IAGTrace includes and defines.  Note that the dbapp.mak makefile will have
// configured us as either OS_LINUX or OS_SOLARIS.

#define TRC_PRODUCT TRC_ID_SAS_DBAPP
#define TRC_GROUP   TRC_COMP_SAS_DBAPP_DBAPP

#include <aiag.h>
#include <atrcapi.h>

#ifdef DEBUG
#define DB_TIME
#endif

// Some respectable books say that you should not put "using" into headers.
// However, this works well for our purposes and saves having to put this
// line into every file that uses the STL.
using namespace std;

extern "C" {
#include <libpq-fe.h>
}

typedef char               int8;
typedef short              int16;
typedef int                int32;
typedef long long          int64;
typedef unsigned char      uint8;
typedef unsigned short     uint16;
typedef unsigned int       uint32;
typedef unsigned long long uint64;

// Exit codes from dbapp - the unlicensed value (2) is used by Craft / dgstart.
#define RC_SUCCESS    0
#define RC_ERROR      1
#define RC_UNLICENSED 2

#ifdef LICENSING
extern "C" {
#include <licincl.h>
}
#endif

#include <dbutil.h>
#include <dbtimers.h>
#include <dbmsg.h>
#include <dbmsgq.h>
#include <dbstatisticsinterface.h>
#include <dbtable.h>
#include <dbtabletginfo.h>
#include <dbupdate.h>
#include <dbmarkertable.h>
#include <dbtrailmgr.h>
#include <dbeventtable.h>
#include <dbstattable.h>
#include <dbtrailcache.h>
#include <dbtrailtable.h>
#include <dbtrailgrouptable.h>
#include <dbtrailmaptable.h>
#include <dbmanager.h>
#include <dbanalyticsmanager.h>
#include <markercache.h>

#ifdef LICENSING
#include <dblicense.h>
#include <dbpolicedog.h>
#endif

#include <dbstatisticsmanager.h>
#include <dbstatisticscollection.h>
#include <dbsocketlistener.h>
#include <dbsocketreceiver.h>
#include <dbmain.h>
#include <dbendtimetable.h>
#include <dbgr303linetable.h>
#include <dbsubglinetable.h>
#include <dbbleslinetable.h>
#include <dbsiplinetable.h>
#include <dbisdnpritable.h>
#include <dbisuptrunktable.h>
#include <dbmftrunktable.h>
#include <dbsiptrunktable.h>
#include <dbsipbindingtable.h>
#include <dbds0table.h>
#include <dbsipcallidtable.h>
#include <dbprotocolerrortable.h>
#include <dbadditionalnumstable.h>
#include <dbphonemactable.h>
#include <dbcallinfotable.h>
#include <dbtestqueriestable.h>
#include <dbmwitargetdnstable.h>
#include <dblsmmonitoringdnstable.h>
#include <dblsmmonitoreddnstable.h>
#include <dboutboundcallinguritable.h>
#include <dbinboundcallinguritable.h>
#include <dboutboundcalleduritable.h>
#include <dbinboundcalleduritable.h>
#include <dbremoteiptable.h>
#include <dbsipallregistertable.h>
#include <dbsipsubscribenotifytable.h>
#include <dbcfednstable.h>
#include <dbcorrelatingmarkertable.h>
#include <dbmovableblocktable.h>
#include <dbv5linetable.h>
#include <dbbroadbandtesttable.h>
#include <dbdntable.h>