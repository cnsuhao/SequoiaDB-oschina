/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef clientTRACE_H__
#define clientTRACE_H__
#define SDB_CLIENT__SETCOLLECTIONINCUR                     0x400000000000L
#define SDB_CURSORIMPL__SETCONNECTIONINCUR                 0x400000000001L
#define SDB_CLIENT__KILLCURSOR                             0x400000000002L
#define SDB_CLIENT__READNEXTBUF                            0x400000000003L
#define SDB_CLIENT_NEXT                                    0x400000000004L
#define SDB_CLIENT_CURRENT                                 0x400000000005L
#define SDB_CLIENT_CLOSECURSOR                             0x400000000006L
#define SDB_CLIENT_UPDATECURRENT                           0x400000000007L
#define SDB_CLIENT_DELCURRENT                              0x400000000008L
#define SDB_CLIMPL__SETNAME                                0x400000000009L
#define SDB_CLIMPL__APPENDOID                              0x40000000000aL
#define SDB_CLIMPL__SETCONNECTIONINCL                      0x40000000000bL
#define SDB_CLIMPL__GETCONNECTIONINCL                      0x40000000000cL
#define SDB_CLIENT_GETCOUNT                                0x40000000000dL
#define SDB_CLIENT_BULKINSERT                              0x40000000000eL
#define SDB_CLIENT_INSERT                                  0x40000000000fL
#define SDB_CLIENT__UPDATE                                 0x400000000010L
#define SDB_CLIENT_DEL                                     0x400000000011L
#define SDB_CLIENT_QUERY                                   0x400000000012L
#define SDB_CLIENT_GETQUERYMETA                            0x400000000013L
#define SDB_CLIENT_RENAME                                  0x400000000014L
#define SDB_CLIENT_CREATEINDEX                             0x400000000015L
#define SDB_CLIENT_GETINDEXES                              0x400000000016L
#define SDB_CLIENT_DROPINDEX                               0x400000000017L
#define SDB_CLIENT_CREATEINCL                              0x400000000018L
#define SDB_CLIENT_DROPINCL                                0x400000000019L
#define SDB_CLIENT_SPLIT                                   0x40000000001aL
#define SDB_CLIENT_SPLIT2                                  0x40000000001bL
#define SDB_CLIENT_SPLITASYNC                              0x40000000001cL
#define SDB_CLIENT_SPLITASYNC2                             0x40000000001dL
#define SDB_CLIENT_AGGREGATE                               0x40000000001eL
#define SDB_CLIENT_ATTACHCOLLECTION                        0x40000000001fL
#define SDB_CLIENT_DETACHCOLLECTION                        0x400000000020L
#define SDB_CLIENT_ALTERCOLLECTION                         0x400000000021L
#define SDB_CLIENT_EXPLAIN                                 0x400000000022L
#define SDB_CLIENT_CREATELOB                               0x400000000023L
#define SDB_CLIENT_REMOVELOB                               0x400000000024L
#define SDB_CLIENT_OPENLOB                                 0x400000000025L
#define SDB_CLIENT_LISTLOBS                                0x400000000026L
#define SDB_CLIENT_RUNCMDOFLOB                             0x400000000027L
#define SDB_CLIENT_GETLOBOBJ                               0x400000000028L
#define SDB_CLIENT_CONNECTINRN                             0x400000000029L
#define SDB_CLIENT_GETSTATUS                               0x40000000002aL
#define SDB_CLIENT__STOPSTART                              0x40000000002bL
#define SDB_CLIENT_GETNODENUM                              0x40000000002cL
#define SDB_CLIENT_CREATENODE                              0x40000000002dL
#define SDB_CLIENT_REMOVENODE                              0x40000000002eL
#define SDB_CLIENT_GETDETAIL                               0x40000000002fL
#define SDB_CLIENT__EXTRACTNODE                            0x400000000030L
#define SDB_CLIENT_GETMASETER                              0x400000000031L
#define SDB_CLIENT_GETSLAVE                                0x400000000032L
#define SDB_CLIENT_GETNODE1                                0x400000000033L
#define SDB_CLIENT_GETNODE2                                0x400000000034L
#define SDB_CLIENT_STARTRS                                 0x400000000035L
#define SDB_CLIENT_STOPRS                                  0x400000000036L
#define SDB_CLIENT__SETNAME                                0x400000000037L
#define SDB_SDBCSIMPL__SETCONNECTIONINCS                   0x400000000038L
#define SDB_CLIENT_GETCOLLECTIONINCS                       0x400000000039L
#define SDB_CLIENT_CREATECOLLECTION                        0x40000000003aL
#define SDB_CLIENT_DROPCOLLECTION                          0x40000000003bL
#define SDB_CLIENT_CREATECS                                0x40000000003cL
#define SDB_CLIENT_DROPCS                                  0x40000000003dL
#define SDB_CLIENT__DOMAINSETCONNECTION                    0x40000000003eL
#define SDB_CLIENT__DOMAINSETNAME                          0x40000000003fL
#define SDB_CLIENT_ALTERDOMAIN                             0x400000000040L
#define SDB_CLIENT_LISTCSINDOMAIN                          0x400000000041L
#define SDB_CLIENT_LISTCLINDOMAIN                          0x400000000042L
#define SDB_CLIENT__SETCONNECTIONINLOB                     0x400000000043L
#define SDB_CLIENT__SETCOLLECTIONINLOB                     0x400000000044L
#define SDB_CLIENT__READINCACHE                            0x400000000045L
#define SDB_CLIENT__REVISEREADLEN                          0x400000000046L
#define SDB_CLIENT__ONCEREAD                               0x400000000047L
#define SDB_CLIENT_CLOSE                                   0x400000000048L
#define SDB_CLIENT_READ                                    0x400000000049L
#define SDB_CLIENT_WRITE                                   0x40000000004aL
#define SDB_CLIENT_SEEK                                    0x40000000004bL
#define SDB_CLIENT_ISCLOSED2                               0x40000000004cL
#define SDB_CLIENT_GETOID2                                 0x40000000004dL
#define SDB_CLIENT_GETSIZE2                                0x40000000004eL
#define SDB_CLIENT_GETCREATETIME2                          0x40000000004fL
#define SDB_CLIENT_ISCLOSED                                0x400000000050L
#define SDB_CLIENT_GETOID                                  0x400000000051L
#define SDB_CLIENT_GETSIZE                                 0x400000000052L
#define SDB_CLIENT_GETCREATETIME                           0x400000000053L
#define SDB_CLIENT__DISCONNECT                             0x400000000054L
#define SDB_CLIENT__CONNECT                                0x400000000055L
#define SDB_CLIENT_CONNECTWITHPORT                         0x400000000056L
#define SDB_CLIENT_CONNECTWITHSERVALADDR                   0x400000000057L
#define SDB_CLIENT_CREATEUSR                               0x400000000058L
#define SDB_CLIENT_REMOVEUSR                               0x400000000059L
#define SDB_CLIENT_GETSNAPSHOT                             0x40000000005aL
#define SDB_CLIENT_RESETSNAPSHOT                           0x40000000005bL
#define SDB_CLIENT_GETLIST                                 0x40000000005cL
#define SDB_CLIENT_CONNECTWITHSERVERNAME                   0x40000000005dL
#define SDB_CLIENT_DISCONNECT                              0x40000000005eL
#define SDB_CLIENT__REALLOCBUFFER                          0x40000000005fL
#define SDB_CLIENT__SEND                                   0x400000000060L
#define SDB_CLIENT__RECV                                   0x400000000061L
#define SDB_CLIENT__RECVEXTRACT                            0x400000000062L
#define SDB_CLIENT__RUNCOMMAND                             0x400000000063L
#define SDB_CLIENT_GETCOLLECTIONINSDB                      0x400000000064L
#define SDB_CLIENT_GETCOLLECTIONSPACE                      0x400000000065L
#define SDB_CLIENT_CREATECOLLECTIONSPACE                   0x400000000066L
#define SDB_CLIENT_CREATECOLLECTIONSPACE2                  0x400000000067L
#define SDB_CLIENT_DROPCOLLECTIONSPACE                     0x400000000068L
#define SDB_CLIENT_GETRGWITHNAME                           0x400000000069L
#define SDB_CLIENT_GETRGWITHID                             0x40000000006aL
#define SDB_CLIENT_CREATERG                                0x40000000006bL
#define SDB_CLIENT_REMOVERG                                0x40000000006cL
#define SDB_CLIENT_CREATECATARG                            0x40000000006dL
#define SDB_CLIENT_ACTIVATERG                              0x40000000006eL
#define SDB_CLIENT_EXECUPDATE                              0x40000000006fL
#define SDB_CLIENT_EXEC                                    0x400000000070L
#define SDB_CLIENT_TRANSBEGIN                              0x400000000071L
#define SDB_CLIENT_TRANSCOMMIT                             0x400000000072L
#define SDB_CLIENT_TRANSROLLBACK                           0x400000000073L
#define SDB_CLIENT_FLUSHCONGIGURE                          0x400000000074L
#define SDB_CLIENT_CRTJSPROCEDURE                          0x400000000075L
#define SDB_CLIENT_RMPROCEDURE                             0x400000000076L
#define SDB_CLIENT_LISTPROCEDURES                          0x400000000077L
#define SDB_CLIENT_EVALJS                                  0x400000000078L
#define SDB_CLIENT_BACKUPOFFLINE                           0x400000000079L
#define SDB_CLIENT_LISTBACKUP                              0x40000000007aL
#define SDB_CLIENT_REMOVEBACKUP                            0x40000000007bL
#define SDB_CLIENT_LISTTASKS                               0x40000000007cL
#define SDB_CLIENT_WAITTASKS                               0x40000000007dL
#define SDB_CLIENT_CANCELTASK                              0x40000000007eL
#define SDB_CLIENT_SETSESSIONATTR                          0x40000000007fL
#define SDB_CLIENT_CLOSE_ALL_CURSORS                       0x400000000080L
#define SDB_CLIENT_IS_VALID2                               0x400000000081L
#define SDB_CLIENT_IS_VALID                                0x400000000082L
#define SDB_CLIENT_CREATEDOMAIN                            0x400000000083L
#define SDB_CLIENT_DROPDOMAIN                              0x400000000084L
#define SDB_CLIENT_GETDOMAIN                               0x400000000085L
#define SDB_CLIENT_LISTDOMAINS                             0x400000000086L
#endif
