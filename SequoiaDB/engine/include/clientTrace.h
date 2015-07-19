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
#define SDB_CLIENT__SETCOLLECTIONINCUR                     0x400000000080L
#define SDB_CURSORIMPL__SETCONNECTIONINCUR                 0x400000000081L
#define SDB_CLIENT__KILLCURSOR                             0x400000000082L
#define SDB_CLIENT__READNEXTBUF                            0x400000000083L
#define SDB_CLIENT_NEXT                                    0x400000000084L
#define SDB_CLIENT_CURRENT                                 0x400000000085L
#define SDB_CLIENT_CLOSECURSOR                             0x400000000086L
#define SDB_CLIENT_UPDATECURRENT                           0x400000000087L
#define SDB_CLIENT_DELCURRENT                              0x400000000088L
#define SDB_CLIMPL__SETNAME                                0x400000000089L
#define SDB_CLIMPL__APPENDOID                              0x40000000008aL
#define SDB_CLIMPL__SETCONNECTIONINCL                      0x40000000008bL
#define SDB_CLIMPL__GETCONNECTIONINCL                      0x40000000008cL
#define SDB_CLIENT_GETCOUNT                                0x40000000008dL
#define SDB_CLIENT_BULKINSERT                              0x40000000008eL
#define SDB_CLIENT_INSERT                                  0x40000000008fL
#define SDB_CLIENT__UPDATE                                 0x400000000090L
#define SDB_CLIENT_DEL                                     0x400000000091L
#define SDB_CLIENT_QUERY                                   0x400000000092L
#define SDB_CLIENT_GETQUERYMETA                            0x400000000093L
#define SDB_CLIENT_RENAME                                  0x400000000094L
#define SDB_CLIENT_CREATEINDEX                             0x400000000095L
#define SDB_CLIENT_GETINDEXES                              0x400000000096L
#define SDB_CLIENT_DROPINDEX                               0x400000000097L
#define SDB_CLIENT_CREATEINCL                              0x400000000098L
#define SDB_CLIENT_DROPINCL                                0x400000000099L
#define SDB_CLIENT_SPLIT                                   0x40000000009aL
#define SDB_CLIENT_SPLIT2                                  0x40000000009bL
#define SDB_CLIENT_SPLITASYNC                              0x40000000009cL
#define SDB_CLIENT_SPLITASYNC2                             0x40000000009dL
#define SDB_CLIENT_AGGREGATE                               0x40000000009eL
#define SDB_CLIENT_ATTACHCOLLECTION                        0x40000000009fL
#define SDB_CLIENT_DETACHCOLLECTION                        0x4000000000a0L
#define SDB_CLIENT_ALTERCOLLECTION                         0x4000000000a1L
#define SDB_CLIENT_EXPLAIN                                 0x4000000000a2L
#define SDB_CLIENT_CREATELOB                               0x4000000000a3L
#define SDB_CLIENT_REMOVELOB                               0x4000000000a4L
#define SDB_CLIENT_OPENLOB                                 0x4000000000a5L
#define SDB_CLIENT_LISTLOBS                                0x4000000000a6L
#define SDB_CLIENT_RUNCMDOFLOB                             0x4000000000a7L
#define SDB_CLIENT_GETLOBOBJ                               0x4000000000a8L
#define SDB_CLIENT_CONNECTINRN                             0x4000000000a9L
#define SDB_CLIENT_GETSTATUS                               0x4000000000aaL
#define SDB_CLIENT__STOPSTART                              0x4000000000abL
#define SDB_CLIENT_GETNODENUM                              0x4000000000acL
#define SDB_CLIENT_CREATENODE                              0x4000000000adL
#define SDB_CLIENT_REMOVENODE                              0x4000000000aeL
#define SDB_CLIENT_GETDETAIL                               0x4000000000afL
#define SDB_CLIENT__EXTRACTNODE                            0x4000000000b0L
#define SDB_CLIENT_GETMASETER                              0x4000000000b1L
#define SDB_CLIENT_GETSLAVE                                0x4000000000b2L
#define SDB_CLIENT_GETNODE1                                0x4000000000b3L
#define SDB_CLIENT_GETNODE2                                0x4000000000b4L
#define SDB_CLIENT_STARTRS                                 0x4000000000b5L
#define SDB_CLIENT_STOPRS                                  0x4000000000b6L
#define SDB_CLIENT__SETNAME                                0x4000000000b7L
#define SDB_SDBCSIMPL__SETCONNECTIONINCS                   0x4000000000b8L
#define SDB_CLIENT_GETCOLLECTIONINCS                       0x4000000000b9L
#define SDB_CLIENT_CREATECOLLECTION                        0x4000000000baL
#define SDB_CLIENT_DROPCOLLECTION                          0x4000000000bbL
#define SDB_CLIENT_CREATECS                                0x4000000000bcL
#define SDB_CLIENT_DROPCS                                  0x4000000000bdL
#define SDB_CLIENT__DOMAINSETCONNECTION                    0x4000000000beL
#define SDB_CLIENT__DOMAINSETNAME                          0x4000000000bfL
#define SDB_CLIENT_ALTERDOMAIN                             0x4000000000c0L
#define SDB_CLIENT_LISTCSINDOMAIN                          0x4000000000c1L
#define SDB_CLIENT_LISTCLINDOMAIN                          0x4000000000c2L
#define SDB_CLIENT__SETCONNECTIONINLOB                     0x4000000000c3L
#define SDB_CLIENT__SETCOLLECTIONINLOB                     0x4000000000c4L
#define SDB_CLIENT__READINCACHE                            0x4000000000c5L
#define SDB_CLIENT__REVISEREADLEN                          0x4000000000c6L
#define SDB_CLIENT__ONCEREAD                               0x4000000000c7L
#define SDB_CLIENT_CLOSE                                   0x4000000000c8L
#define SDB_CLIENT_READ                                    0x4000000000c9L
#define SDB_CLIENT_WRITE                                   0x4000000000caL
#define SDB_CLIENT_SEEK                                    0x4000000000cbL
#define SDB_CLIENT_ISCLOSED2                               0x4000000000ccL
#define SDB_CLIENT_GETOID2                                 0x4000000000cdL
#define SDB_CLIENT_GETSIZE2                                0x4000000000ceL
#define SDB_CLIENT_GETCREATETIME2                          0x4000000000cfL
#define SDB_CLIENT_ISCLOSED                                0x4000000000d0L
#define SDB_CLIENT_GETOID                                  0x4000000000d1L
#define SDB_CLIENT_GETSIZE                                 0x4000000000d2L
#define SDB_CLIENT_GETCREATETIME                           0x4000000000d3L
#define SDB_CLIENT__DISCONNECT                             0x4000000000d4L
#define SDB_CLIENT__CONNECT                                0x4000000000d5L
#define SDB_CLIENT_CONNECTWITHPORT                         0x4000000000d6L
#define SDB_CLIENT_CONNECTWITHSERVALADDR                   0x4000000000d7L
#define SDB_CLIENT_CREATEUSR                               0x4000000000d8L
#define SDB_CLIENT_REMOVEUSR                               0x4000000000d9L
#define SDB_CLIENT_GETSNAPSHOT                             0x4000000000daL
#define SDB_CLIENT_RESETSNAPSHOT                           0x4000000000dbL
#define SDB_CLIENT_GETLIST                                 0x4000000000dcL
#define SDB_CLIENT_CONNECTWITHSERVERNAME                   0x4000000000ddL
#define SDB_CLIENT_DISCONNECT                              0x4000000000deL
#define SDB_CLIENT__REALLOCBUFFER                          0x4000000000dfL
#define SDB_CLIENT__SEND                                   0x4000000000e0L
#define SDB_CLIENT__RECV                                   0x4000000000e1L
#define SDB_CLIENT__RECVEXTRACT                            0x4000000000e2L
#define SDB_CLIENT__RUNCOMMAND                             0x4000000000e3L
#define SDB_CLIENT_GETCOLLECTIONINSDB                      0x4000000000e4L
#define SDB_CLIENT_GETCOLLECTIONSPACE                      0x4000000000e5L
#define SDB_CLIENT_CREATECOLLECTIONSPACE                   0x4000000000e6L
#define SDB_CLIENT_CREATECOLLECTIONSPACE2                  0x4000000000e7L
#define SDB_CLIENT_DROPCOLLECTIONSPACE                     0x4000000000e8L
#define SDB_CLIENT_GETRGWITHNAME                           0x4000000000e9L
#define SDB_CLIENT_GETRGWITHID                             0x4000000000eaL
#define SDB_CLIENT_CREATERG                                0x4000000000ebL
#define SDB_CLIENT_REMOVERG                                0x4000000000ecL
#define SDB_CLIENT_CREATECATARG                            0x4000000000edL
#define SDB_CLIENT_ACTIVATERG                              0x4000000000eeL
#define SDB_CLIENT_EXECUPDATE                              0x4000000000efL
#define SDB_CLIENT_EXEC                                    0x4000000000f0L
#define SDB_CLIENT_TRANSBEGIN                              0x4000000000f1L
#define SDB_CLIENT_TRANSCOMMIT                             0x4000000000f2L
#define SDB_CLIENT_TRANSROLLBACK                           0x4000000000f3L
#define SDB_CLIENT_FLUSHCONGIGURE                          0x4000000000f4L
#define SDB_CLIENT_CRTJSPROCEDURE                          0x4000000000f5L
#define SDB_CLIENT_RMPROCEDURE                             0x4000000000f6L
#define SDB_CLIENT_LISTPROCEDURES                          0x4000000000f7L
#define SDB_CLIENT_EVALJS                                  0x4000000000f8L
#define SDB_CLIENT_BACKUPOFFLINE                           0x4000000000f9L
#define SDB_CLIENT_LISTBACKUP                              0x4000000000faL
#define SDB_CLIENT_REMOVEBACKUP                            0x4000000000fbL
#define SDB_CLIENT_LISTTASKS                               0x4000000000fcL
#define SDB_CLIENT_WAITTASKS                               0x4000000000fdL
#define SDB_CLIENT_CANCELTASK                              0x4000000000feL
#define SDB_CLIENT_SETSESSIONATTR                          0x4000000000ffL
#define SDB_CLIENT_CLOSE_ALL_CURSORS                       0x400000000100L
#define SDB_CLIENT_IS_VALID2                               0x400000000101L
#define SDB_CLIENT_IS_VALID                                0x400000000102L
#define SDB_CLIENT_CREATEDOMAIN                            0x400000000103L
#define SDB_CLIENT_DROPDOMAIN                              0x400000000104L
#define SDB_CLIENT_GETDOMAIN                               0x400000000105L
#define SDB_CLIENT_LISTDOMAINS                             0x400000000106L
#endif
