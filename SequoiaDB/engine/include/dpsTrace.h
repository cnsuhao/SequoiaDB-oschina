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
#define SDB__DPSLOGFILE_INIT                               0x10000002caL
#define SDB__DPSLOGFILE__RESTRORE                          0x10000002cbL
#define SDB__DPSLOGFILE_RESET                              0x10000002ccL
#define SDB__DPSLOGFILE__FLUSHHD                           0x10000002cdL
#define SDB__DPSLOGFILE__RDHD                              0x10000002ceL
#define SDB__DPSLOGFILE_WRITE                              0x10000002cfL
#define SDB__DPSLOGFILE_READ                               0x10000002d0L
#define SDB__DPSLGFILEMGR_INIT                             0x10000002d1L
#define SB__DPSLGFILEMGR__ANLYS                            0x10000002d2L
#define SDB__DPSLGFILEMGR_FLUSH                            0x10000002d3L
#define SDB__DPSLGFILEMGR_LOAD                             0x10000002d4L
#define SDB__DPSLGFILEMGR_MOVE                             0x10000002d5L
#define SDB__DPSLGPAGE                                     0x10000002d6L
#define SDB__DPSLGPAGE2                                    0x10000002d7L
#define SDB__DPSLGRECD_LOAD                                0x10000002d8L
#define SDB__DPSLGRECD_FIND                                0x10000002d9L
#define SDB__DPSLGRECD_PUSH                                0x10000002daL
#define SDB__DPSLGRECD_DUMP                                0x10000002dbL
#define SDB__DPSLGWRAPP_RECDROW                            0x10000002dcL
#define SDB__DPSLGWRAPP_PREPARE                            0x10000002ddL
#define SDB__DPSMGBLK_CLEAR                                0x10000002deL
#define SDB__DPSMSGBLK_EXTEND                              0x10000002dfL
#define SDB__DPS_INSERT2RECORD                             0x10000002e0L
#define SDB_DPS_INSERT2RECORD                              0x10000002e1L
#define SDB__DPS_UPDATE2RECORD                             0x10000002e2L
#define SDB__DPS_RECORD2UPDATE                             0x10000002e3L
#define SDB__DPS_DELETE2RECORD                             0x10000002e4L
#define SDB__DPS_RECORD2DELETE                             0x10000002e5L
#define SDB__DPS_CSCRT2RECORD                              0x10000002e6L
#define SDB__DPS_RECORD2CSCRT                              0x10000002e7L
#define SDB__DPS_CSDEL2RECORD                              0x10000002e8L
#define SDB__DPS_RECORD2CSDEL                              0x10000002e9L
#define SDB__DPS_CLCRT2RECORD                              0x10000002eaL
#define SDB__DPS_RECORD2CLCRT                              0x10000002ebL
#define SDB__DPS_CLDEL2RECORD                              0x10000002ecL
#define SDB__DPS_RECORD2CLDEL                              0x10000002edL
#define SDB__DPS_IXCRT2RECORD                              0x10000002eeL
#define SDB__DPS_RECORD2IXCRT                              0x10000002efL
#define SDB__DPS_IXDEL2RECORD                              0x10000002f0L
#define SDB__DPS_RECORD2IXDEL                              0x10000002f1L
#define SDB__DPS_CLRENAME2RECORD                           0x10000002f2L
#define SDB__DPS_RECORD2CLRENAME                           0x10000002f3L
#define SDB__DPS_CLTRUNC2RECORD                            0x10000002f4L
#define SDB__DPS_RECORD2CLTRUNC                            0x10000002f5L
#define SDB__DPS_RECORD2TRANSCOMMIT                        0x10000002f6L
#define SDB__DPS_TRANSCOMMIT2RECORD                        0x10000002f7L
#define SDB__DPS_TRANSROLLBACK2RECORD                      0x10000002f8L
#define SDB__DPS_INVALIDCATA2RECORD                        0x10000002f9L
#define SDB__DPS_RECORD2INVALIDCATA                        0x10000002faL
#define SDB__DPS_LOBW2RECORD                               0x10000002fbL
#define SDB__DPS_RECORD2LOBW                               0x10000002fcL
#define SDB__DPS_LOBU2RECORD                               0x10000002fdL
#define SDB__DPS_RECORD2LOBU                               0x10000002feL
#define SDB__DPS_LOBRM2RECORD                              0x10000002ffL
#define SDB__DPS_RECORD2LOBRM                              0x1000000300L
#define SDB__DPS_LOBTRUNCATE2RECORD                        0x1000000301L
#define SDB__DPS_RECORD2LOBTRUNCATE                        0x1000000302L
#define SDB__DPSRPCMGR_INIT                                0x1000000303L
#define SDB__DPSRPCMGR__RESTRORE                           0x1000000304L
#define SDB__DPSRPCMGR_PREPAGES                            0x1000000305L
#define SDB__DPSRPCMGR_WRITEDATA                           0x1000000306L
#define SDB__DPSRPCMGR_MERGE                               0x1000000307L
#define SDB__DPSRPCMGR_GETLSNWIN                           0x1000000308L
#define SDB__DPSRPCMGR_GETLSNWIN2                          0x1000000309L
#define SDB__DPSRPCMGR__MVPAGES                            0x100000030aL
#define SDB__DPSRPCMGR_MOVE                                0x100000030bL
#define SDB__DPSRPCMGR__GETSTARTLSN                        0x100000030cL
#define SDB__DPSRPCMGR__SEARCH                             0x100000030dL
#define SDB__DPSRPCMGR__PARSE                              0x100000030eL
#define SDB__DPSRPCMGR_SEARCH                              0x100000030fL
#define SDB__DPSRPCMGR__ALLOCATE                           0x1000000310L
#define SDB__DPSRPCMGR__PSH2SNDQUEUE                       0x1000000311L
#define SDB__DPSRPCMGR__MRGLOGS                            0x1000000312L
#define SDB__DPSRPCMGR__MRGPAGE                            0x1000000313L
#define SDB__DPSRPCMGR_RUN                                 0x1000000314L
#define SDB__DPSRPCMGR__FLUSHALL                           0x1000000315L
#define SDB__DPSRPCMGR_TEARDOWN                            0x1000000316L
#define SDB__DPSRPCMGR__FLUSHPAGE                          0x1000000317L
#define SDB_DPSTRANSCB_SVTRANSINFO                         0x1000000318L
#define SDB_DPSTRANSCB_ADDTRANSCB                          0x1000000319L
#define SDB_DPSTRANSCB_DELTRANSCB                          0x100000031aL
#define SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG                0x100000031bL
#define SDB_DPSTRANSCB_TERMALLTRANS                        0x100000031cL
#define SDB_DPSTRANSLOCK_ACQUIREX                          0x100000031dL
#define SDB_DPSTRANSLOCK_ACQUIRES                          0x100000031eL
#define SDB_DPSTRANSLOCK_ACQUIREIX                         0x100000031fL
#define SDB_DPSTRANSLOCK_ACQUIREIS                         0x1000000320L
#define SDB_DPSTRANSLOCK_RELEASE                           0x1000000321L
#define SDB_DPSTRANSLOCK_RELEASEALL                        0x1000000322L
#define SDB_DPSTRANSLOCK_UPGRADE                           0x1000000323L
#define SDB_DPSTRANSLOCK_UPGRADECHECK                      0x1000000324L
#define SDB_DPSTRANSLOCK_GETBUCKET                         0x1000000325L
#define SDB_DPSTRANSLOCK_TESTS                             0x1000000326L
#define SDB_DPSTRANSLOCK_TESTUPGRADE                       0x1000000327L
#define SDB_DPSTRANSLOCK_TESTIS                            0x1000000328L
#define SDB_DPSTRANSLOCK_TESTX                             0x1000000329L
#define SDB_DPSTRANSLOCK_TESTIX                            0x100000032aL
#define SDB_DPSTRANSLOCK_TRYX                              0x100000032bL
#define SDB_DPSTRANSLOCK_TRYS                              0x100000032cL
#define SDB_DPSTRANSLOCK_TRYIX                             0x100000032dL
#define SDB_DPSTRANSLOCK_TRYIS                             0x100000032eL
#define SDB_DPSTRANSLOCK_TRYUPGRADE                        0x100000032fL
#define SDB_DPSTRANSLOCK_TRYUPGRADEORAPPEND                0x1000000330L
#define SDB_DPSTRANSLOCK_TRYORAPPENDX                      0x1000000331L
#define SDB_DPSTRANSLOCK_WAIT                              0x1000000332L
#define SDB_DPSTRANSLOCK_HASWAIT                           0x1000000333L
#define SDB_DPSLOCKBUCKET_ACQUIRE                          0x1000000334L
#define SDB_DPSLOCKBUCKET_WAITLOCKX                        0x1000000335L
#define SDB_DPSLOCKBUCKET_UPGRADE                          0x1000000336L
#define SDB_DPSLOCKBUCKET_LOCKID                           0x1000000337L
#define SDB_DPSLOCKBUCKET_APPENDTORUN                      0x1000000338L
#define SDB_DPSLOCKBUCKET_APPENDTOWAIT                     0x1000000339L
#define SDB_DPSLOCKBUCKET_APPENDHEADTOWAIT                 0x100000033aL
#define SDB_DPSLOCKBUCKET_REMOVEFROMRUN                    0x100000033bL
#define SDB_DPSLOCKBUCKET_REMOVEFROMWAIT                   0x100000033cL
#define SDB_DPSLOCKBUCKET_WAITLOCK                         0x100000033dL
#define SDB_DPSLOCKBUCKET_WAKEUP                           0x100000033eL
#define SDB_DPSLOCKBUCKET_CHECKCOMPATIBLE                  0x100000033fL
#define SDB_DPSLOCKBUCKET_TEST                             0x1000000340L
#define SDB_DPSLOCKBUCKET_TRYACQUIRE                       0x1000000341L
#define SDB_DPSLOCKBUCKET_TRYACQUIREORAPPEND               0x1000000342L
#define SDB_DPSLOCKBUCKET_HASWAIT                          0x1000000343L
#endif
