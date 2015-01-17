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
#ifndef dpsTRACE_H__
#define dpsTRACE_H__
#define SDB_DPSTRANSLOCK_ACQUIREX                          0x1000000087L
#define SDB_DPSTRANSLOCK_ACQUIRES                          0x1000000088L
#define SDB_DPSTRANSLOCK_ACQUIREIX                         0x1000000089L
#define SDB_DPSTRANSLOCK_ACQUIREIS                         0x100000008aL
#define SDB_DPSTRANSLOCK_RELEASE                           0x100000008bL
#define SDB_DPSTRANSLOCK_RELEASEALL                        0x100000008cL
#define SDB_DPSTRANSLOCK_UPGRADE                           0x100000008dL
#define SDB_DPSTRANSLOCK_UPGRADECHECK                      0x100000008eL
#define SDB_DPSTRANSLOCK_GETBUCKET                         0x100000008fL
#define SDB_DPSTRANSLOCK_TESTS                             0x1000000090L
#define SDB_DPSTRANSLOCK_TESTUPGRADE                       0x1000000091L
#define SDB_DPSTRANSLOCK_TESTIS                            0x1000000092L
#define SDB_DPSTRANSLOCK_TESTX                             0x1000000093L
#define SDB_DPSTRANSLOCK_TESTIX                            0x1000000094L
#define SDB_DPSTRANSLOCK_TRYX                              0x1000000095L
#define SDB_DPSTRANSLOCK_TRYS                              0x1000000096L
#define SDB_DPSTRANSLOCK_TRYIX                             0x1000000097L
#define SDB_DPSTRANSLOCK_TRYIS                             0x1000000098L
#define SDB_DPSTRANSLOCK_TRYUPGRADE                        0x1000000099L
#define SDB_DPSTRANSLOCK_TRYUPGRADEORAPPEND                0x100000009aL
#define SDB_DPSTRANSLOCK_TRYORAPPENDX                      0x100000009bL
#define SDB_DPSTRANSLOCK_WAIT                              0x100000009cL
#define SDB_DPSTRANSLOCK_HASWAIT                           0x100000009dL
#define SDB__DPSLOGFILE_INIT                               0x100000009eL
#define SDB__DPSLOGFILE__RESTRORE                          0x100000009fL
#define SDB__DPSLOGFILE_RESET                              0x10000000a0L
#define SDB__DPSLOGFILE__FLUSHHD                           0x10000000a1L
#define SDB__DPSLOGFILE__RDHD                              0x10000000a2L
#define SDB__DPSLOGFILE_WRITE                              0x10000000a3L
#define SDB__DPSLOGFILE_READ                               0x10000000a4L
#define SDB__DPSLGFILEMGR_INIT                             0x10000000a5L
#define SB__DPSLGFILEMGR__ANLYS                            0x10000000a6L
#define SDB__DPSLGFILEMGR_FLUSH                            0x10000000a7L
#define SDB__DPSLGFILEMGR_LOAD                             0x10000000a8L
#define SDB__DPSLGFILEMGR_MOVE                             0x10000000a9L
#define SDB__DPSLGWRAPP_RECDROW                            0x10000000aaL
#define SDB__DPSLGWRAPP_PREPARE                            0x10000000abL
#define SDB_DPSTRANSCB_SVTRANSINFO                         0x10000000acL
#define SDB_DPSTRANSCB_ADDTRANSCB                          0x10000000adL
#define SDB_DPSTRANSCB_DELTRANSCB                          0x10000000aeL
#define SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG                0x10000000afL
#define SDB_DPSTRANSCB_TERMALLTRANS                        0x10000000b0L
#define SDB_DPSLOCKBUCKET_ACQUIRE                          0x10000000b1L
#define SDB_DPSLOCKBUCKET_WAITLOCKX                        0x10000000b2L
#define SDB_DPSLOCKBUCKET_UPGRADE                          0x10000000b3L
#define SDB_DPSLOCKBUCKET_LOCKID                           0x10000000b4L
#define SDB_DPSLOCKBUCKET_APPENDTORUN                      0x10000000b5L
#define SDB_DPSLOCKBUCKET_APPENDTOWAIT                     0x10000000b6L
#define SDB_DPSLOCKBUCKET_APPENDHEADTOWAIT                 0x10000000b7L
#define SDB_DPSLOCKBUCKET_REMOVEFROMRUN                    0x10000000b8L
#define SDB_DPSLOCKBUCKET_REMOVEFROMWAIT                   0x10000000b9L
#define SDB_DPSLOCKBUCKET_WAITLOCK                         0x10000000baL
#define SDB_DPSLOCKBUCKET_WAKEUP                           0x10000000bbL
#define SDB_DPSLOCKBUCKET_CHECKCOMPATIBLE                  0x10000000bcL
#define SDB_DPSLOCKBUCKET_TEST                             0x10000000bdL
#define SDB_DPSLOCKBUCKET_TRYACQUIRE                       0x10000000beL
#define SDB_DPSLOCKBUCKET_TRYACQUIREORAPPEND               0x10000000bfL
#define SDB_DPSLOCKBUCKET_HASWAIT                          0x10000000c0L
#define SDB__DPSLGRECD_LOAD                                0x10000000c1L
#define SDB__DPSLGRECD_FIND                                0x10000000c2L
#define SDB__DPSLGRECD_PUSH                                0x10000000c3L
#define SDB__DPSLGRECD_DUMP                                0x10000000c4L
#define SDB__DPSMGBLK_CLEAR                                0x10000000c5L
#define SDB__DPS_INSERT2RECORD                             0x10000000c6L
#define SDB_DPS_INSERT2RECORD                              0x10000000c7L
#define SDB__DPS_UPDATE2RECORD                             0x10000000c8L
#define SDB__DPS_RECORD2UPDATE                             0x10000000c9L
#define SDB__DPS_DELETE2RECORD                             0x10000000caL
#define SDB__DPS_RECORD2DELETE                             0x10000000cbL
#define SDB__DPS_CSCRT2RECORD                              0x10000000ccL
#define SDB__DPS_RECORD2CSCRT                              0x10000000cdL
#define SDB__DPS_CSDEL2RECORD                              0x10000000ceL
#define SDB__DPS_RECORD2CSDEL                              0x10000000cfL
#define SDB__DPS_CLCRT2RECORD                              0x10000000d0L
#define SDB__DPS_RECORD2CLCRT                              0x10000000d1L
#define SDB__DPS_CLDEL2RECORD                              0x10000000d2L
#define SDB__DPS_RECORD2CLDEL                              0x10000000d3L
#define SDB__DPS_IXCRT2RECORD                              0x10000000d4L
#define SDB__DPS_RECORD2IXCRT                              0x10000000d5L
#define SDB__DPS_IXDEL2RECORD                              0x10000000d6L
#define SDB__DPS_RECORD2IXDEL                              0x10000000d7L
#define SDB__DPS_CLRENAME2RECORD                           0x10000000d8L
#define SDB__DPS_RECORD2CLRENAME                           0x10000000d9L
#define SDB__DPS_CLTRUNC2RECORD                            0x10000000daL
#define SDB__DPS_RECORD2CLTRUNC                            0x10000000dbL
#define SDB__DPS_RECORD2TRANSCOMMIT                        0x10000000dcL
#define SDB__DPS_TRANSCOMMIT2RECORD                        0x10000000ddL
#define SDB__DPS_TRANSROLLBACK2RECORD                      0x10000000deL
#define SDB__DPS_INVALIDCATA2RECORD                        0x10000000dfL
#define SDB__DPS_RECORD2INVALIDCATA                        0x10000000e0L
#define SDB__DPS_LOBW2RECORD                               0x10000000e1L
#define SDB__DPS_RECORD2LOBW                               0x10000000e2L
#define SDB__DPS_LOBU2RECORD                               0x10000000e3L
#define SDB__DPS_RECORD2LOBU                               0x10000000e4L
#define SDB__DPS_LOBRM2RECORD                              0x10000000e5L
#define SDB__DPS_RECORD2LOBRM                              0x10000000e6L
#define SDB__DPS_LOBTRUNCATE2RECORD                        0x10000000e7L
#define SDB__DPS_RECORD2LOBTRUNCATE                        0x10000000e8L
#define SDB__DPSMSGBLK_EXTEND                              0x10000000e9L
#define SDB__DPSLGPAGE                                     0x10000000eaL
#define SDB__DPSLGPAGE2                                    0x10000000ebL
#define SDB__DPSRPCMGR_INIT                                0x10000000ecL
#define SDB__DPSRPCMGR__RESTRORE                           0x10000000edL
#define SDB__DPSRPCMGR_PREPAGES                            0x10000000eeL
#define SDB__DPSRPCMGR_WRITEDATA                           0x10000000efL
#define SDB__DPSRPCMGR_MERGE                               0x10000000f0L
#define SDB__DPSRPCMGR_GETLSNWIN                           0x10000000f1L
#define SDB__DPSRPCMGR_GETLSNWIN2                          0x10000000f2L
#define SDB__DPSRPCMGR__MVPAGES                            0x10000000f3L
#define SDB__DPSRPCMGR_MOVE                                0x10000000f4L
#define SDB__DPSRPCMGR__GETSTARTLSN                        0x10000000f5L
#define SDB__DPSRPCMGR__SEARCH                             0x10000000f6L
#define SDB__DPSRPCMGR__PARSE                              0x10000000f7L
#define SDB__DPSRPCMGR_SEARCH                              0x10000000f8L
#define SDB__DPSRPCMGR__ALLOCATE                           0x10000000f9L
#define SDB__DPSRPCMGR__PSH2SNDQUEUE                       0x10000000faL
#define SDB__DPSRPCMGR__MRGLOGS                            0x10000000fbL
#define SDB__DPSRPCMGR__MRGPAGE                            0x10000000fcL
#define SDB__DPSRPCMGR_RUN                                 0x10000000fdL
#define SDB__DPSRPCMGR__FLUSHALL                           0x10000000feL
#define SDB__DPSRPCMGR_TEARDOWN                            0x10000000ffL
#define SDB__DPSRPCMGR__FLUSHPAGE                          0x1000000100L
#endif
