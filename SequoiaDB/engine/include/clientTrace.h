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
#define SDB_CLIENT__SETCOLLECTIONINCUR                     0x40000000057cL
#define SDB_CURSORIMPL__SETCONNECTIONINCUR                 0x40000000057dL
#define SDB_CLIENT__KILLCURSOR                             0x40000000057eL
#define SDB_CLIENT__READNEXTBUF                            0x40000000057fL
#define SDB_CLIENT_NEXT                                    0x400000000580L
#define SDB_CLIENT_CURRENT                                 0x400000000581L
#define SDB_CLIENT_CLOSECURSOR                             0x400000000582L
#define SDB_CLIENT_UPDATECURRENT                           0x400000000583L
#define SDB_CLIENT_DELCURRENT                              0x400000000584L
#define SDB_CLIMPL__SETNAME                                0x400000000585L
#define SDB_CLIMPL__APPENDOID                              0x400000000586L
#define SDB_CLIMPL__SETCONNECTIONINCL                      0x400000000587L
#define SDB_CLIMPL__GETCONNECTIONINCL                      0x400000000588L
#define SDB_CLIENT_GETCOUNT                                0x400000000589L
#define SDB_CLIENT_BULKINSERT                              0x40000000058aL
#define SDB_CLIENT_INSERT                                  0x40000000058bL
#define SDB_CLIENT__UPDATE                                 0x40000000058cL
#define SDB_CLIENT_DEL                                     0x40000000058dL
#define SDB_CLIENT_QUERY                                   0x40000000058eL
#define SDB_CLIENT_GETQUERYMETA                            0x40000000058fL
#define SDB_CLIENT_RENAME                                  0x400000000590L
#define SDB_CLIENT_CREATEINDEX                             0x400000000591L
#define SDB_CLIENT_GETINDEXES                              0x400000000592L
#define SDB_CLIENT_DROPINDEX                               0x400000000593L
#define SDB_CLIENT_CREATEINCL                              0x400000000594L
#define SDB_CLIENT_DROPINCL                                0x400000000595L
#define SDB_CLIENT_SPLIT                                   0x400000000596L
#define SDB_CLIENT_SPLIT2                                  0x400000000597L
#define SDB_CLIENT_SPLITASYNC                              0x400000000598L
#define SDB_CLIENT_SPLITASYNC2                             0x400000000599L
#define SDB_CLIENT_AGGREGATE                               0x40000000059aL
#define SDB_CLIENT_ATTACHCOLLECTION                        0x40000000059bL
#define SDB_CLIENT_DETACHCOLLECTION                        0x40000000059cL
#define SDB_CLIENT_ALTERCOLLECTION                         0x40000000059dL
#define SDB_CLIENT_EXPLAIN                                 0x40000000059eL
#define SDB_CLIENT_CREATELOB                               0x40000000059fL
#define SDB_CLIENT_REMOVELOB                               0x4000000005a0L
#define SDB_CLIENT_OPENLOB                                 0x4000000005a1L
#define SDB_CLIENT_LISTLOBS                                0x4000000005a2L
#define SDB_CLIENT_RUNCMDOFLOB                             0x4000000005a3L
#define SDB_CLIENT_GETLOBOBJ                               0x4000000005a4L
#define SDB_CLIENT_CONNECTINRN                             0x4000000005a5L
#define SDB_CLIENT_GETSTATUS                               0x4000000005a6L
#define SDB_CLIENT__STOPSTART                              0x4000000005a7L
#define SDB_CLIENT_GETNODENUM                              0x4000000005a8L
#define SDB_CLIENT_CREATENODE                              0x4000000005a9L
#define SDB_CLIENT_REMOVENODE                              0x4000000005aaL
#define SDB_CLIENT_GETDETAIL                               0x4000000005abL
#define SDB_CLIENT__EXTRACTNODE                            0x4000000005acL
#define SDB_CLIENT_GETMASETER                              0x4000000005adL
#define SDB_CLIENT_GETSLAVE                                0x4000000005aeL
#define SDB_CLIENT_GETNODE1                                0x4000000005afL
#define SDB_CLIENT_GETNODE2                                0x4000000005b0L
#define SDB_CLIENT_STARTRS                                 0x4000000005b1L
#define SDB_CLIENT_STOPRS                                  0x4000000005b2L
#define SDB_CLIENT__SETNAME                                0x4000000005b3L
#define SDB_SDBCSIMPL__SETCONNECTIONINCS                   0x4000000005b4L
#define SDB_CLIENT_GETCOLLECTIONINCS                       0x4000000005b5L
#define SDB_CLIENT_CREATECOLLECTION                        0x4000000005b6L
#define SDB_CLIENT_DROPCOLLECTION                          0x4000000005b7L
#define SDB_CLIENT_CREATECS                                0x4000000005b8L
#define SDB_CLIENT_DROPCS                                  0x4000000005b9L
#define SDB_CLIENT__DOMAINSETCONNECTION                    0x4000000005baL
#define SDB_CLIENT__DOMAINSETNAME                          0x4000000005bbL
#define SDB_CLIENT_ALTERDOMAIN                             0x4000000005bcL
#define SDB_CLIENT_LISTCSINDOMAIN                          0x4000000005bdL
#define SDB_CLIENT_LISTCLINDOMAIN                          0x4000000005beL
#define SDB_CLIENT__SETCONNECTIONINLOB                     0x4000000005bfL
#define SDB_CLIENT__SETCOLLECTIONINLOB                     0x4000000005c0L
#define SDB_CLIENT__READINCACHE                            0x4000000005c1L
#define SDB_CLIENT__REVISEREADLEN                          0x4000000005c2L
#define SDB_CLIENT__ONCEREAD                               0x4000000005c3L
#define SDB_CLIENT_CLOSE                                   0x4000000005c4L
#define SDB_CLIENT_READ                                    0x4000000005c5L
#define SDB_CLIENT_WRITE                                   0x4000000005c6L
#define SDB_CLIENT_SEEK                                    0x4000000005c7L
#define SDB_CLIENT_ISCLOSED2                               0x4000000005c8L
#define SDB_CLIENT_GETOID2                                 0x4000000005c9L
#define SDB_CLIENT_GETSIZE2                                0x4000000005caL
#define SDB_CLIENT_GETCREATETIME2                          0x4000000005cbL
#define SDB_CLIENT_ISCLOSED                                0x4000000005ccL
#define SDB_CLIENT_GETOID                                  0x4000000005cdL
#define SDB_CLIENT_GETSIZE                                 0x4000000005ceL
#define SDB_CLIENT_GETCREATETIME                           0x4000000005cfL
#define SDB_CLIENT__DISCONNECT                             0x4000000005d0L
#define SDB_CLIENT__CONNECT                                0x4000000005d1L
#define SDB_CLIENT_CONNECTWITHPORT                         0x4000000005d2L
#define SDB_CLIENT_CONNECTWITHSERVALADDR                   0x4000000005d3L
#define SDB_CLIENT_CREATEUSR                               0x4000000005d4L
#define SDB_CLIENT_REMOVEUSR                               0x4000000005d5L
#define SDB_CLIENT_GETSNAPSHOT                             0x4000000005d6L
#define SDB_CLIENT_RESETSNAPSHOT                           0x4000000005d7L
#define SDB_CLIENT_GETLIST                                 0x4000000005d8L
#define SDB_CLIENT_CONNECTWITHSERVERNAME                   0x4000000005d9L
#define SDB_CLIENT_DISCONNECT                              0x4000000005daL
#define SDB_CLIENT__REALLOCBUFFER                          0x4000000005dbL
#define SDB_CLIENT__SEND                                   0x4000000005dcL
#define SDB_CLIENT__RECV                                   0x4000000005ddL
#define SDB_CLIENT__RECVEXTRACT                            0x4000000005deL
#define SDB_CLIENT__RUNCOMMAND                             0x4000000005dfL
#define SDB_CLIENT_GETCOLLECTIONINSDB                      0x4000000005e0L
#define SDB_CLIENT_GETCOLLECTIONSPACE                      0x4000000005e1L
#define SDB_CLIENT_CREATECOLLECTIONSPACE                   0x4000000005e2L
#define SDB_CLIENT_CREATECOLLECTIONSPACE2                  0x4000000005e3L
#define SDB_CLIENT_DROPCOLLECTIONSPACE                     0x4000000005e4L
#define SDB_CLIENT_GETRGWITHNAME                           0x4000000005e5L
#define SDB_CLIENT_GETRGWITHID                             0x4000000005e6L
#define SDB_CLIENT_CREATERG                                0x4000000005e7L
#define SDB_CLIENT_REMOVERG                                0x4000000005e8L
#define SDB_CLIENT_CREATECATARG                            0x4000000005e9L
#define SDB_CLIENT_ACTIVATERG                              0x4000000005eaL
#define SDB_CLIENT_EXECUPDATE                              0x4000000005ebL
#define SDB_CLIENT_EXEC                                    0x4000000005ecL
#define SDB_CLIENT_TRANSBEGIN                              0x4000000005edL
#define SDB_CLIENT_TRANSCOMMIT                             0x4000000005eeL
#define SDB_CLIENT_TRANSROLLBACK                           0x4000000005efL
#define SDB_CLIENT_FLUSHCONGIGURE                          0x4000000005f0L
#define SDB_CLIENT_CRTJSPROCEDURE                          0x4000000005f1L
#define SDB_CLIENT_RMPROCEDURE                             0x4000000005f2L
#define SDB_CLIENT_LISTPROCEDURES                          0x4000000005f3L
#define SDB_CLIENT_EVALJS                                  0x4000000005f4L
#define SDB_CLIENT_BACKUPOFFLINE                           0x4000000005f5L
#define SDB_CLIENT_LISTBACKUP                              0x4000000005f6L
#define SDB_CLIENT_REMOVEBACKUP                            0x4000000005f7L
#define SDB_CLIENT_LISTTASKS                               0x4000000005f8L
#define SDB_CLIENT_WAITTASKS                               0x4000000005f9L
#define SDB_CLIENT_CANCELTASK                              0x4000000005faL
#define SDB_CLIENT_SETSESSIONATTR                          0x4000000005fbL
#define SDB_CLIENT_CLOSE_ALL_CURSORS                       0x4000000005fcL
#define SDB_CLIENT_IS_VALID2                               0x4000000005fdL
#define SDB_CLIENT_IS_VALID                                0x4000000005feL
#define SDB_CLIENT_CREATEDOMAIN                            0x4000000005ffL
#define SDB_CLIENT_DROPDOMAIN                              0x400000000600L
#define SDB_CLIENT_GETDOMAIN                               0x400000000601L
#define SDB_CLIENT_LISTDOMAINS                             0x400000000602L
#endif
