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
#define SDB_CLIENT__SETCOLLECTIONINCUR                     0x40000000061dL
#define SDB_CURSORIMPL__SETCONNECTIONINCUR                 0x40000000061eL
#define SDB_CLIENT__KILLCURSOR                             0x40000000061fL
#define SDB_CLIENT__READNEXTBUF                            0x400000000620L
#define SDB_CLIENT_NEXT                                    0x400000000621L
#define SDB_CLIENT_CURRENT                                 0x400000000622L
#define SDB_CLIENT_CLOSECURSOR                             0x400000000623L
#define SDB_CLIENT_UPDATECURRENT                           0x400000000624L
#define SDB_CLIENT_DELCURRENT                              0x400000000625L
#define SDB_CLIMPL__SETNAME                                0x400000000626L
#define SDB_CLIMPL__APPENDOID                              0x400000000627L
#define SDB_CLIMPL__SETCONNECTIONINCL                      0x400000000628L
#define SDB_CLIMPL__GETCONNECTIONINCL                      0x400000000629L
#define SDB_CLIENT_GETCOUNT                                0x40000000062aL
#define SDB_CLIENT_BULKINSERT                              0x40000000062bL
#define SDB_CLIENT_INSERT                                  0x40000000062cL
#define SDB_CLIENT__UPDATE                                 0x40000000062dL
#define SDB_CLIENT_DEL                                     0x40000000062eL
#define SDB_CLIENT_QUERY                                   0x40000000062fL
#define SDB_CLIENT_GETQUERYMETA                            0x400000000630L
#define SDB_CLIENT_RENAME                                  0x400000000631L
#define SDB_CLIENT_CREATEINDEX                             0x400000000632L
#define SDB_CLIENT_GETINDEXES                              0x400000000633L
#define SDB_CLIENT_DROPINDEX                               0x400000000634L
#define SDB_CLIENT_CREATEINCL                              0x400000000635L
#define SDB_CLIENT_DROPINCL                                0x400000000636L
#define SDB_CLIENT_SPLIT                                   0x400000000637L
#define SDB_CLIENT_SPLIT2                                  0x400000000638L
#define SDB_CLIENT_SPLITASYNC                              0x400000000639L
#define SDB_CLIENT_SPLITASYNC2                             0x40000000063aL
#define SDB_CLIENT_AGGREGATE                               0x40000000063bL
#define SDB_CLIENT_ATTACHCOLLECTION                        0x40000000063cL
#define SDB_CLIENT_DETACHCOLLECTION                        0x40000000063dL
#define SDB_CLIENT_ALTERCOLLECTION                         0x40000000063eL
#define SDB_CLIENT_EXPLAIN                                 0x40000000063fL
#define SDB_CLIENT_CREATELOB                               0x400000000640L
#define SDB_CLIENT_REMOVELOB                               0x400000000641L
#define SDB_CLIENT_OPENLOB                                 0x400000000642L
#define SDB_CLIENT_LISTLOBS                                0x400000000643L
#define SDB_CLIENT_RUNCMDOFLOB                             0x400000000644L
#define SDB_CLIENT_GETLOBOBJ                               0x400000000645L
#define SDB_CLIENT_CONNECTINRN                             0x400000000646L
#define SDB_CLIENT_GETSTATUS                               0x400000000647L
#define SDB_CLIENT__STOPSTART                              0x400000000648L
#define SDB_CLIENT_GETNODENUM                              0x400000000649L
#define SDB_CLIENT_CREATENODE                              0x40000000064aL
#define SDB_CLIENT_REMOVENODE                              0x40000000064bL
#define SDB_CLIENT_GETDETAIL                               0x40000000064cL
#define SDB_CLIENT__EXTRACTNODE                            0x40000000064dL
#define SDB_CLIENT_GETMASETER                              0x40000000064eL
#define SDB_CLIENT_GETSLAVE                                0x40000000064fL
#define SDB_CLIENT_GETNODE1                                0x400000000650L
#define SDB_CLIENT_GETNODE2                                0x400000000651L
#define SDB_CLIENT_STARTRS                                 0x400000000652L
#define SDB_CLIENT_STOPRS                                  0x400000000653L
#define SDB_CLIENT__SETNAME                                0x400000000654L
#define SDB_SDBCSIMPL__SETCONNECTIONINCS                   0x400000000655L
#define SDB_CLIENT_GETCOLLECTIONINCS                       0x400000000656L
#define SDB_CLIENT_CREATECOLLECTION                        0x400000000657L
#define SDB_CLIENT_DROPCOLLECTION                          0x400000000658L
#define SDB_CLIENT_CREATECS                                0x400000000659L
#define SDB_CLIENT_DROPCS                                  0x40000000065aL
#define SDB_CLIENT__DOMAINSETCONNECTION                    0x40000000065bL
#define SDB_CLIENT__DOMAINSETNAME                          0x40000000065cL
#define SDB_CLIENT_ALTERDOMAIN                             0x40000000065dL
#define SDB_CLIENT_LISTCSINDOMAIN                          0x40000000065eL
#define SDB_CLIENT_LISTCLINDOMAIN                          0x40000000065fL
#define SDB_CLIENT__SETCONNECTIONINLOB                     0x400000000660L
#define SDB_CLIENT__SETCOLLECTIONINLOB                     0x400000000661L
#define SDB_CLIENT__READINCACHE                            0x400000000662L
#define SDB_CLIENT__REVISEREADLEN                          0x400000000663L
#define SDB_CLIENT__ONCEREAD                               0x400000000664L
#define SDB_CLIENT_CLOSE                                   0x400000000665L
#define SDB_CLIENT_READ                                    0x400000000666L
#define SDB_CLIENT_WRITE                                   0x400000000667L
#define SDB_CLIENT_SEEK                                    0x400000000668L
#define SDB_CLIENT_ISCLOSED2                               0x400000000669L
#define SDB_CLIENT_GETOID2                                 0x40000000066aL
#define SDB_CLIENT_GETSIZE2                                0x40000000066bL
#define SDB_CLIENT_GETCREATETIME2                          0x40000000066cL
#define SDB_CLIENT_ISCLOSED                                0x40000000066dL
#define SDB_CLIENT_GETOID                                  0x40000000066eL
#define SDB_CLIENT_GETSIZE                                 0x40000000066fL
#define SDB_CLIENT_GETCREATETIME                           0x400000000670L
#define SDB_CLIENT__DISCONNECT                             0x400000000671L
#define SDB_CLIENT__CONNECT                                0x400000000672L
#define SDB_CLIENT_CONNECTWITHPORT                         0x400000000673L
#define SDB_CLIENT_CONNECTWITHSERVALADDR                   0x400000000674L
#define SDB_CLIENT_CREATEUSR                               0x400000000675L
#define SDB_CLIENT_REMOVEUSR                               0x400000000676L
#define SDB_CLIENT_GETSNAPSHOT                             0x400000000677L
#define SDB_CLIENT_RESETSNAPSHOT                           0x400000000678L
#define SDB_CLIENT_GETLIST                                 0x400000000679L
#define SDB_CLIENT_CONNECTWITHSERVERNAME                   0x40000000067aL
#define SDB_CLIENT_DISCONNECT                              0x40000000067bL
#define SDB_CLIENT__REALLOCBUFFER                          0x40000000067cL
#define SDB_CLIENT__SEND                                   0x40000000067dL
#define SDB_CLIENT__RECV                                   0x40000000067eL
#define SDB_CLIENT__RECVEXTRACT                            0x40000000067fL
#define SDB_CLIENT__RUNCOMMAND                             0x400000000680L
#define SDB_CLIENT_GETCOLLECTIONINSDB                      0x400000000681L
#define SDB_CLIENT_GETCOLLECTIONSPACE                      0x400000000682L
#define SDB_CLIENT_CREATECOLLECTIONSPACE                   0x400000000683L
#define SDB_CLIENT_CREATECOLLECTIONSPACE2                  0x400000000684L
#define SDB_CLIENT_DROPCOLLECTIONSPACE                     0x400000000685L
#define SDB_CLIENT_GETRGWITHNAME                           0x400000000686L
#define SDB_CLIENT_GETRGWITHID                             0x400000000687L
#define SDB_CLIENT_CREATERG                                0x400000000688L
#define SDB_CLIENT_REMOVERG                                0x400000000689L
#define SDB_CLIENT_CREATECATARG                            0x40000000068aL
#define SDB_CLIENT_ACTIVATERG                              0x40000000068bL
#define SDB_CLIENT_EXECUPDATE                              0x40000000068cL
#define SDB_CLIENT_EXEC                                    0x40000000068dL
#define SDB_CLIENT_TRANSBEGIN                              0x40000000068eL
#define SDB_CLIENT_TRANSCOMMIT                             0x40000000068fL
#define SDB_CLIENT_TRANSROLLBACK                           0x400000000690L
#define SDB_CLIENT_FLUSHCONGIGURE                          0x400000000691L
#define SDB_CLIENT_CRTJSPROCEDURE                          0x400000000692L
#define SDB_CLIENT_RMPROCEDURE                             0x400000000693L
#define SDB_CLIENT_LISTPROCEDURES                          0x400000000694L
#define SDB_CLIENT_EVALJS                                  0x400000000695L
#define SDB_CLIENT_BACKUPOFFLINE                           0x400000000696L
#define SDB_CLIENT_LISTBACKUP                              0x400000000697L
#define SDB_CLIENT_REMOVEBACKUP                            0x400000000698L
#define SDB_CLIENT_LISTTASKS                               0x400000000699L
#define SDB_CLIENT_WAITTASKS                               0x40000000069aL
#define SDB_CLIENT_CANCELTASK                              0x40000000069bL
#define SDB_CLIENT_SETSESSIONATTR                          0x40000000069cL
#define SDB_CLIENT_CLOSE_ALL_CURSORS                       0x40000000069dL
#define SDB_CLIENT_IS_VALID2                               0x40000000069eL
#define SDB_CLIENT_IS_VALID                                0x40000000069fL
#define SDB_CLIENT_CREATEDOMAIN                            0x4000000006a0L
#define SDB_CLIENT_DROPDOMAIN                              0x4000000006a1L
#define SDB_CLIENT_GETDOMAIN                               0x4000000006a2L
#define SDB_CLIENT_LISTDOMAINS                             0x4000000006a3L
#endif
