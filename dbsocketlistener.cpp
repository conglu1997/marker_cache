/* dbsocketlistener.cpp */
/**MOD+***********************************************************************/
/* Module:    dbsocketlistener.cpp                                           */
/*                                                                           */
/* Purpose:   Socket listener.                                               */
/*                                                                           */
/* (C) COPYRIGHT DATA CONNECTION LIMITED 2007 - 2008                         */
/*                                                                           */
/* $Id:: dbsocketlistener.cpp 182799 2015-12-10 09:51:16Z jw2              $ */
/* $URL:: http://enfieldsvn/repos/metaswitch/trunk/sas/code/ced/dbapp/dbso#$ */
/*                                                                           */
/**MOD-***********************************************************************/

#define TRC_FILE "dbsocketlistener"
#include <dbinc.h>

#ifdef OS_LINUX
#define SCKOS_SOCK_SOCKLEN socklen_t
#else
#define SCKOS_SOCK_SOCKLEN int
#endif

extern "C" void* db_receiver_thread_wrapper(void* context);

extern void db_listener_thread();

extern "C"
{
  void* db_listener_thread_wrapper(void* context)
  {
    db_listener_thread();
    return(NULL);
  }
}

void db_listener_thread()
{
  TRC_BEGIN_FN("db_listener_thread");

  while (1)
  {
    db_socket_listener* db_listener;

    TRC_NRM(("Create socket listener"));
    try
    {
      db_listener = new db_socket_listener;
    }
    catch (int e)
    {
      TRC_ERR_SYSLOG("Failed to start listening process - terminating");
      exit(RC_ERROR);
    }

    TRC_NRM(("Start socket listener"));
    try
    {
      db_listener->listener();
    }
    catch (int e)
    {
      TRC_ERR_SYSLOG("Exception on socket listener (%d).  Restarting listener", e);
      delete db_listener;
    }
  }

  TRC_END_FN();
}

db_socket_listener::db_socket_listener()
{
  TRC_BEGIN_FN("db_socket_listener");
  int rc;
  SCKOS_SOCK_SOCKLEN inane;
  int sock_size = 1048576; // 1 Megabyte

  // Configure socket based on IP version:
  _so = socket( AF_INET6, SOCK_STREAM, 0 );

  // Verify socket opened:
  if( _so == -1)
  {
    TRC_ERR_SYSLOG("Can't open socket for listening (%s)", strerror(errno));
    throw 1;
  }

  // This setting is probably redundant.
  //    * We don't use socket reuse in SAS
  //    * Socket reuse is not really portable between Solaris and Linux anyway,
  //      so probably wouldn’t work if we did try to use it.
  // See http://goo.gl/yVTsHh for details.
  int reuse = 1;
  rc = setsockopt(_so, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  if (rc < 0)
  {
    TRC_ERR_SYSLOG("Failed to set SO_REUSEADDR option (%s)\n", strerror(errno));
    throw 1;
  }

  /***************************************************************************/
  /* Set the socket size to be 1 Megabyte.                                   */
  /***************************************************************************/
  setsockopt(_so, SOL_SOCKET, SO_RCVBUF, &sock_size, sizeof(sock_size));
  memset(&_ip_addr6, 0, sizeof(_ip_addr6));

  // DBAPP needs to listen on both IPv4 and IPv6 addresses.  To achieve this we
  // only need to open an IPv6 socket.  The sockets layer handles IPv4 for us
  // by mapping IPv4 connections to IPv6 competability addresses
  // (i.e. ::ffff:x.x.x.x)
  _ip_addr6.sin6_family = AF_INET6;
  _ip_addr6.sin6_port = htons(sas_port);

  // Apparently no one knows yet what to do with sin6_flowinfo. The RFC has
  // little explanation on it. Some sources suggest just setting it to 0.
  // http://goo.gl/PgyxA5
  _ip_addr6.sin6_flowinfo = 0;
  _ip_addr6.sin6_addr = in6addr_any;

  // Scope ID is for referencing a link-local (NIC) address, so is therefore 0
  // for any remote address.
  _ip_addr6.sin6_scope_id = 0;

  rc = ::bind(_so, (struct sockaddr *)&_ip_addr6, sizeof(_ip_addr6));

  if (rc != 0)
  {
    TRC_ERR_SYSLOG("Bind(port = %d/%d) failed %d %d (%s)",
                   sas_port,
                   _ip_addr6.sin6_port,
                   rc,
                   errno,
                   strerror(errno));
    throw 1;
  }

  inane = sizeof(_ip_addr6);
  rc = getsockname(_so, (struct sockaddr *)&_ip_addr6, &inane);

  if (rc != 0)
  {
    TRC_ERR_SYSLOG("getsockname() failed (%s)", strerror(errno));
    throw 1;
  }

  char v6_addr_printable[INET6_ADDRSTRLEN];

  inet_ntop(AF_INET6,
            &(_ip_addr6.sin6_addr),
            v6_addr_printable,
            INET6_ADDRSTRLEN);

  TRC_NRM(("Created socket listener for IP address %s, port %d/%d",
          v6_addr_printable,
          sas_port,
          _ip_addr6.sin6_port));

  TRC_END_FN();
}

db_socket_listener::~db_socket_listener()
{
  TRC_BEGIN_FN("~db_socket_listener");

  char v6_addr_printable[INET6_ADDRSTRLEN];

  inet_ntop(AF_INET6,
            &(_ip_addr6.sin6_addr),
            v6_addr_printable,
            INET6_ADDRSTRLEN);

  TRC_NRM(("Closing socket listener for IP address %s, port %d/%d",
          v6_addr_printable,
          sas_port,
          _ip_addr6.sin6_port));

  close(_so);

  TRC_END_FN();
}

void db_socket_listener::listener()
{
  TRC_BEGIN_FN("listener");

  int rc;
  SCKOS_SOCK_SOCKLEN inane;
  intptr_t in_so;

  struct sockaddr_in6 in_ip_addr;
  pthread_t sock_thread_id;

  char v6_addr_printable[INET6_ADDRSTRLEN];

  TRC_NRM_SYSLOG("Listening on IP address %s, port %d",
                inet_ntop(AF_INET6,&(_ip_addr6.sin6_addr),
                v6_addr_printable,
                INET6_ADDRSTRLEN),
                ntohs(_ip_addr6.sin6_port));

  while (1)
  {
    rc = listen(_so, 5);

    if (rc != 0)
    {
      TRC_ERR_SYSLOG("listen() failed for socket IP address %s, port %x (%s)",
                    inet_ntop(AF_INET6,
                              &(_ip_addr6.sin6_addr),
                              v6_addr_printable,
                              INET6_ADDRSTRLEN),
                    ntohs(_ip_addr6.sin6_port),
                    strerror(errno));
      TRC_ERR_SYSLOG("dbapp terminating");
      exit(RC_ERROR);
    }

    inane = sizeof(in_ip_addr);

    // Accept returns int -1 on error, and a non-negative socket ID on success
    in_so = accept(_so, (struct sockaddr *)&in_ip_addr, &inane);

    // Check the first bits of the ip address.  If the first 80 are 0, and the
    // next 16 are 1, then this address is of the form ::ffff:x.x.x.x - an
    // IPv6-mapped IPv4 address.
    unsigned char is_ipv4 =  in_ip_addr.sin6_addr.s6_addr[0]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[1]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[2]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[3]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[4]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[5]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[6]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[7]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[8]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[9]  == 0x0  &&
                            in_ip_addr.sin6_addr.s6_addr[10] == 0xff &&
                            in_ip_addr.sin6_addr.s6_addr[11] == 0xff;

    // Log the accepted IP address in the desired format:
    if(is_ipv4)
    {
      inet_ntop(AF_INET6,
                &(in_ip_addr.sin6_addr),
                (v6_addr_printable),
                INET6_ADDRSTRLEN);

      // Skip the first 7 chars '::ffff:' and print
      char *ipv4_addr_printable = v6_addr_printable+7;
      TRC_NRM_ALWAYS(("Accepted IPv4 address %s, port %d",
                      ipv4_addr_printable,
                      ntohs(in_ip_addr.sin6_port)));
    }
    else
    {
      TRC_NRM_ALWAYS(("Accepted IPv6 address %s, port %d",
                      inet_ntop(AF_INET6,
                                &(in_ip_addr.sin6_addr),
                                (v6_addr_printable),
                                INET6_ADDRSTRLEN),
                      ntohs(in_ip_addr.sin6_port)));
    }

    TRC_NRM_ALWAYS(("Always: Accepted IP address %s, port %d",
                    inet_ntop(AF_INET6,
                              &(in_ip_addr.sin6_addr),
                              (v6_addr_printable),
                              INET6_ADDRSTRLEN),
                    ntohs(in_ip_addr.sin6_port)));

    rc = pthread_create(&(sock_thread_id),
                        NULL,
                        db_receiver_thread_wrapper,
                        (void *)in_so);
    if (rc == 0)
    {
      TRC_NRM(("Created socket receiver thread %u", sock_thread_id));
      rc = pthread_detach(sock_thread_id);
    }
    else
    {
      TRC_ERR_SYSLOG("Failed to create dbapp socket receiver thread (error %d) "
                     "- system will be ignored", rc);
    }
  }

  TRC_END_FN();
}
